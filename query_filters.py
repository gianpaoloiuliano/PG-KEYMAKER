import re
from dataclasses import dataclass, field

import subpattern_extractor_v2 as sub_ext


# ---------------------------------------------------------------------------
# FILTRI SULLE QUERY
# ---------------------------------------------------------------------------
# I filtri sono una stringa Cypher libera, es.:
#   "p.age < 18 AND en1.cost IS NOT NULL AND avg(en1.cost) > 100 PER p"
#
# - la stringa viene spezzata sugli AND di primo livello (fuori da parentesi
#   e stringhe): ogni pezzo è un filtro
# - filtro SEMPLICE: condizione Cypher sugli alias del pattern, finisce nel WHERE
# - filtro AGGREGATO: contiene avg/min/max/sum/count/... e DEVE terminare con
#   "PER <alias_nodo>" (alias di raggruppamento). L'aggregato viene calcolato
#   per ogni <alias_nodo> sulla porzione contigua minima del pattern che
#   contiene <alias_nodo> e gli alias usati dentro l'aggregato (scope).
#   Diventa quindi un filtro sul solo nodo <alias_nodo>.
#
# Un filtro si applica a un nodo dell'albero (foglia, tripla, subpattern) se
# tutti i suoi alias compaiono in quel nodo.

# Livello delle stampe [FILTRI]:
#   0 → nessuna stampa
#   1 → riepilogo: filtri letti, a quanti nodi dell'albero si applicano, occorrenze
#   2 → dettaglio: in più query generate, righe per nodo, esito APOC, cache foglie
DEBUG_FILTRI = 1

_AGG_FUNCS = r'avg|min|max|sum|count|collect|stDevP|stDev|percentileCont|percentileDisc'
_AGG_RE = re.compile(rf'\b({_AGG_FUNCS})\s*\(', re.IGNORECASE)
_PER_RE = re.compile(r'^(.*\S)\s+PER\s+([A-Za-z_]\w*)\s*$', re.IGNORECASE | re.DOTALL)
_STRING_RE = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
# identificatori non preceduti da '.' (così 'p.age' dà 'p' e non 'age')
_IDENT_RE = re.compile(r'(?<![\w.$])([A-Za-z_]\w*)\b')


def dbg(msg, level=1):
    if DEBUG_FILTRI >= level:
        print(f"[FILTRI] {msg}")


@dataclass
class Filter:
    text: str                                   # testo originale del filtro
    aliases: set = field(default_factory=set)   # alias che servono per applicarlo
    is_aggregate: bool = False
    group_alias: str = None                     # solo aggregati
    agg_calls: list = field(default_factory=list)  # [(nome_var, "avg(en1.cost)"), ...]
    cond: str = None                            # condizione sulle var aggregate
    scope_pattern: str = None                   # pattern su cui calcolare l'aggregato
    ok_var: str = None                          # lista dei group_alias validi
    idx: int = None                             # numero del filtro (#k nelle stampe)

    def __repr__(self):
        if self.is_aggregate:
            return f"Filter(AGG per {self.group_alias}: {self.text!r})"
        return f"Filter({self.text!r})"


# ---------------------------------------------------------------------------
# PARSING
# ---------------------------------------------------------------------------

def _mask_strings(s: str) -> str:
    """Sostituisce il contenuto delle stringhe letterali con spazi (stessa lunghezza)."""
    return _STRING_RE.sub(lambda m: ' ' * len(m.group(0)), s)


def split_top_level_and(s: str) -> list[str]:
    """Spezza s sugli AND fuori da parentesi e stringhe letterali."""
    masked = _mask_strings(s)
    parts = []
    depth = 0
    start = 0
    i = 0
    n = len(masked)
    while i < n:
        c = masked[i]
        if c in '([{':
            depth += 1
        elif c in ')]}':
            depth -= 1
        elif depth == 0 and masked[i:i + 3].upper() == 'AND':
            before = masked[i - 1] if i > 0 else ' '
            after = masked[i + 3] if i + 3 < n else ' '
            if not (before.isalnum() or before == '_') and not (after.isalnum() or after == '_'):
                parts.append(s[start:i])
                start = i + 3
                i += 3
                continue
        i += 1
    parts.append(s[start:])
    return [p.strip() for p in parts if p.strip()]


def _find_aliases(text: str, known_aliases) -> set:
    masked = _mask_strings(text)
    return {m.group(1) for m in _IDENT_RE.finditer(masked) if m.group(1) in known_aliases}


# namespace di funzioni Cypher che si scrivono come "x.y(...)" e non sono alias
_CYPHER_NAMESPACES = {'apoc', 'date', 'datetime', 'localdatetime', 'localtime', 'time',
                      'duration', 'point', 'math', 'string', 'db', 'gds'}
_PROP_ACCESS_RE = re.compile(r'(?<![\w.$])([A-Za-z_]\w*)\s*\.')


def _check_unknown_aliases(text: str, known_aliases):
    masked = _mask_strings(text)
    unknown = {m.group(1) for m in _PROP_ACCESS_RE.finditer(masked)
               if m.group(1) not in known_aliases and m.group(1).lower() not in _CYPHER_NAMESPACES}
    if unknown:
        raise ValueError(
            f"Nel filtro {text!r} gli alias {sorted(unknown)} non compaiono nel pattern "
            f"(alias disponibili: {sorted(known_aliases)})"
        )


def _extract_agg_calls(text: str, prefix: str):
    """
    Trova le chiamate aggregate (con parentesi bilanciate) e le sostituisce con
    variabili. Ritorna (testo_con_variabili, [(nome_var, chiamata), ...]).
    """
    masked = _mask_strings(text)
    calls = []
    out = []
    last = 0
    for m in _AGG_RE.finditer(masked):
        if m.start() < last:
            continue  # aggregato annidato dentro un altro: resta nella chiamata esterna
        depth = 0
        j = m.end() - 1  # posizione della '('
        while j < len(masked):
            if masked[j] == '(':
                depth += 1
            elif masked[j] == ')':
                depth -= 1
                if depth == 0:
                    break
            j += 1
        if depth != 0:
            raise ValueError(f"Parentesi non bilanciate nel filtro: {text!r}")
        var = f"{prefix}_{len(calls)}"
        calls.append((var, text[m.start():j + 1]))
        out.append(text[last:m.start()])
        out.append(var)
        last = j + 1
    out.append(text[last:])
    return ''.join(out), calls


def _scope_pattern(tokens, alias_index, aliases) -> str:
    """Porzione contigua minima del pattern che contiene tutti gli alias dati."""
    idx = [alias_index[a] for a in aliases]
    lo, hi = min(idx), max(idx)
    # la porzione deve iniziare e finire con un nodo (indici pari)
    if lo % 2 == 1:
        lo -= 1
    if hi % 2 == 1:
        hi += 1
    return sub_ext.tokens_to_pattern(tokens[lo:hi + 1])


def parse_filters(filter_str, pattern) -> list[Filter]:
    """Trasforma la stringa di filtri in una lista di Filter, validata sul pattern."""
    if not filter_str or not filter_str.strip():
        return []

    tokens = sub_ext.tokenize_pattern(pattern)
    alias_index = sub_ext.tokens_alias_index(tokens)
    node_aliases = {a for a, i in alias_index.items() if i % 2 == 0}

    filters = []
    for k, part in enumerate(split_top_level_and(filter_str)):
        _check_unknown_aliases(part, alias_index)
        if _AGG_RE.search(_mask_strings(part)):
            m = _PER_RE.match(part)
            if not m:
                raise ValueError(
                    f"Filtro aggregato senza 'PER <alias>': {part!r} "
                    f"(es. 'avg(en.cost) > 100 PER p')"
                )
            body, group = m.group(1), m.group(2)
            if group not in node_aliases:
                raise ValueError(
                    f"Alias di raggruppamento '{group}' non è un nodo del pattern "
                    f"(nodi disponibili: {sorted(node_aliases)})"
                )
            cond, calls = _extract_agg_calls(body, f"__agg{k}")
            inner_aliases = set()
            for _, call in calls:
                inner_aliases |= _find_aliases(call, alias_index)
            # alias fuori dagli aggregati: ammesso solo il gruppo
            outer_aliases = _find_aliases(cond, alias_index)
            if outer_aliases - {group}:
                raise ValueError(
                    f"Nel filtro aggregato {part!r} gli alias {sorted(outer_aliases - {group})} "
                    f"sono usati fuori dall'aggregato: è ammesso solo l'alias di gruppo '{group}'"
                )
            f = Filter(
                text=part,
                aliases={group},
                is_aggregate=True,
                group_alias=group,
                agg_calls=calls,
                cond=cond.strip(),
                scope_pattern=_scope_pattern(tokens, alias_index, inner_aliases | {group}),
                ok_var=f"__ok{k}",
            )
            dbg(f"#{k} aggregato  {part}   (per {group}, calcolato su {f.scope_pattern})")
        else:
            aliases = _find_aliases(part, alias_index)
            f = Filter(text=part, aliases=aliases)
            dbg(f"#{k} semplice   {part}   (alias {', '.join(sorted(aliases))})")
        f.idx = k
        filters.append(f)
    return filters


# ---------------------------------------------------------------------------
# APPLICABILITÀ / CACHE
# ---------------------------------------------------------------------------


def applicable_filters(filters, aliases) -> list[Filter]:
    """Filtri i cui alias sono tutti contenuti in `aliases` (usato dalla baseline GFD)."""
    return [f for f in (filters or []) if f.aliases <= set(aliases)]


def filters_for_subpattern(filters, sub_str) -> list[Filter]:
    """
    Filtri applicabili a un sotto-pattern dato come stringa (usato dalle baseline
    HyUCC e HPIValid): arco singolo "[r:R]", nodo singolo "(a:A)" o cammino.
    """
    if not filters:
        return []
    sub_str = sub_str.strip()
    if sub_str.startswith('['):
        aliases = {sub_ext._parse_edge_token(sub_str)[0]}
    else:
        aliases = set(sub_ext.tokens_alias_index(sub_ext.tokenize_pattern(sub_str)))
    return applicable_filters(filters, aliases)


def return_star(pattern_str, filters) -> str:
    """
    Sostituto di "RETURN *" per le baseline HyUCC e HPIValid: con filtri aggregati
    "*" restituirebbe anche le liste __okK delle subquery, che finirebbero come
    colonne nel DataFrame. In quel caso ritorna gli alias del pattern, altrimenti "*".
    """
    if not any(f.is_aggregate for f in (filters or [])):
        return "*"
    return ", ".join(sub_ext.tokens_alias_index(sub_ext.tokenize_pattern(pattern_str.strip())))


def filters_label(filters) -> str:
    """
    Suffisso per le stampe del pattern in analisi, es. "  [filtri #0 p.gender = 'F' | #2 count(en1) > 100 PER p]".
    Stringa vuota se il nodo non ha filtri o se DEBUG_FILTRI = 0.
    """
    if not filters or DEBUG_FILTRI < 1:
        return ""
    return "  [filtri " + " | ".join(f"#{f.idx} {f.text}" for f in filters) + "]"


def filter_signature(filters, alias) -> str:
    """
    Firma dei filtri di una foglia con l'alias sostituito da un segnaposto.
    Due foglie con la stessa label possono condividere le chiavi solo se
    hanno la stessa firma. Nessun filtro → "".
    """
    if not filters:
        return ""
    alias_re = re.compile(rf'(?<![\w.$]){re.escape(alias)}\b')
    return " AND ".join(sorted(alias_re.sub('§', f.text) for f in filters))


def baseline_subpatterns(pattern) -> list[str]:
    """
    Sotto-pattern usati dalle baseline HyUCC e HPIValid, ottenuti con lo stesso
    tokenizer di PG-KEYMAKER (supporta -[r:R]-, -[r:R]->, <-[r:R]-):
      - singoli nodi "(a:A)" e singoli archi "[r:R]"
      - tutti i cammini contigui nodo→nodo con almeno un arco, pattern intero
        compreso, con le frecce originali preservate
    """
    tokens = sub_ext.tokenize_pattern(pattern.strip())
    subs = [t for t in tokens[0::2]]
    subs += [sub_ext.connector_raw_token(t) for t in tokens[1::2]]
    for start in range(0, len(tokens), 2):
        for end in range(start + 2, len(tokens), 2):
            subs.append(sub_ext.tokens_to_pattern(tokens[start:end + 1]))
    return list(dict.fromkeys(subs))


# ---------------------------------------------------------------------------
# COSTRUZIONE QUERY
# ---------------------------------------------------------------------------

def build_match_block(pattern, filters, extra_where=None) -> str:
    """
    Costruisce la parte MATCH ... WHERE ... di una query.
    Senza filtri e senza extra_where ritorna esattamente "MATCH <pattern>".

    Ogni filtro aggregato diventa una subquery isolata eseguita una sola volta,
    che raccoglie i valori validi dell'alias di gruppo:
        CALL () { MATCH <scope> WITH g, avg(...) AS __aggK_0 WHERE <cond> RETURN collect(g) AS __okK }
    e nel WHERE principale compare "g IN __okK".
    """
    filters = filters or []
    lines = []
    where = []

    for f in filters:
        if f.is_aggregate:
            g = f.group_alias
            with_items = ", ".join([g] + [f"{call} AS {var}" for var, call in f.agg_calls])
            lines.append(
                f"CALL () {{ MATCH {f.scope_pattern} WITH {with_items} "
                f"WHERE {f.cond} RETURN collect({g}) AS {f.ok_var} }}"
            )
            where.append(f"{g} IN {f.ok_var}")
        else:
            where.append(f"({f.text})")

    if extra_where:
        where.append(extra_where)

    lines.append(f"MATCH {pattern}")
    if where:
        lines.append("WHERE " + " AND ".join(where))

    block = "\n".join(lines)
    if filters:
        dbg(f"query per {pattern}:\n{block}", level=2)
    return block


# ---------------------------------------------------------------------------
# QUICK SELF-TEST (senza database)
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import neo4j_querying_module as n4j

    pattern = "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)"
    filtri = ("p.gender = 'F' AND (en1.cost > 10 OR en1.cost IS NULL) AND nx.days < 30 "
              "AND avg(en1.cost) > 100 PER p AND count(pro) >= 2 PER en2 AND pro.description <> 'AND'")

    fs = parse_filters(filtri, pattern)
    print()
    G = sub_ext.build_pattern_graph(pattern, fs)
    print()
    for node in ["(p:Patient)", "[nx:NEXT]", "(en2:Encounter)",
                 "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)", pattern]:
        print("=" * 70)
        print(n4j.generate_query_from_pattern(node, G.nodes[node]["filters"]))
    print("=" * 70)
    print("firma en1:", filter_signature(G.nodes["(en1:Encounter)"]["filters"], "en1"))
    print("firma en2:", filter_signature(G.nodes["(en2:Encounter)"]["filters"], "en2"))

    for bad in ["avg(en1.cost) > 100", "zz.age > 3 AND max(en1.cost) > 1 PER has"]:
        try:
            parse_filters(bad, pattern)
        except ValueError as e:
            print("errore atteso:", e)
