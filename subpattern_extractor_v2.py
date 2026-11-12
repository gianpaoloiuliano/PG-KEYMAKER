import re
import networkx as nx
import matplotlib.pyplot as plt


# ---------------------------------------------------------------------------
# PATTERN TOKENIZER
# ---------------------------------------------------------------------------
# Un pattern Cypher-like è una sequenza alternata:
#   NODE  CONNECTOR  NODE  CONNECTOR  NODE  ...
#
# dove NODE  = "(alias:Label)"
#       CONNECTOR = uno tra:
#           -[r:R]-    undirected
#           -[r:R]->   right-directed
#           <-[r:R]-   left-directed
#
# La regex cattura separatamente:
#   group 1 → "nodo"      es.  "(a:Account)"
#   group 2 → "connector" es.  "-[wd:Withdraw]->"  oppure "<-[t:Tran]-"
#
# NOTA: i token vengono restituiti in ordine, alternati (nodo, conn, nodo, ...)

_TOKEN_RE = re.compile(
    r'(\([^)]*\))'           # nodo  →  (alias:Label)  (possibilmente con label multipli)
    r'|'
    r'(<-\[[^\]]*\]-)'       # ←  left-directed connector
    r'|'
    r'(-\[[^\]]*\]->)'       # →  right-directed connector
    r'|'
    r'(-\[[^\]]*\]-)'        # -   undirected connector
)


def tokenize_pattern(pattern: str) -> list[str]:
    """
    Scompone un pattern nelle sue parti atomiche:
      - token di tipo 'node'      → "(alias:Label)"
      - token di tipo 'connector' → "-[r:R]-"  /  "-[r:R]->"  /  "<-[r:R]-"

    Ritorna la lista ordinata dei token (nodo, conn, nodo, conn, nodo ...).
    Lancia ValueError se il pattern non ha forma valida.
    """
    tokens = [m.group(0) for m in _TOKEN_RE.finditer(pattern)]

    # Validazione minima: deve iniziare e finire con un nodo
    if not tokens:
        raise ValueError(f"Pattern non riconosciuto: {pattern!r}")
    if not (tokens[0].startswith('(') and tokens[-1].startswith('(')):
        raise ValueError(
            f"Il pattern deve iniziare e finire con un nodo '(...)': {pattern!r}"
        )
    # Lunghezza: 1, 3, 5, 7 ... (dispari)
    if len(tokens) % 2 == 0:
        raise ValueError(
            f"Il pattern non è ben formato (atteso numero dispari di token): {pattern!r}"
        )
    return tokens


def connector_direction(connector: str) -> str:
    """
    Ritorna la direzione di un connector:
      '>'  → da nodo sinistro a nodo destro   (es. -[r:R]->)
      '<'  → da nodo destro  a nodo sinistro  (es. <-[r:R]-)
      ''   → non direzionato                  (es. -[r:R]-)
    """
    if connector.endswith('->'):
        return '>'
    elif connector.startswith('<-'):
        return '<'
    return ''


def connector_raw_token(connector: str) -> str:
    """
    Estrae la parte '[alias:Label]' da un connector (strip delle frecce '-' e '<>').
    Usato solo per costruire la chiave del nodo-arco nel grafo interno.
    """
    # Togli prefisso "<-" o "-" e suffisso "->" o "-"
    inner = re.search(r'\[([^\]]*)\]', connector)
    if inner:
        return f"[{inner.group(1)}]"
    return connector


def tokens_to_pattern(tokens: list[str]) -> str:
    """
    Ri-assembla i token in un unico pattern stringa
    (identico all'originale se i token provengono da tokenize_pattern).
    """
    return ''.join(tokens)


# ---------------------------------------------------------------------------
# SUBPATTERN EXTRACTION
# ---------------------------------------------------------------------------

def extract_all_valid_subpatterns(tokens: list[str]) -> list[list[str]]:
    """
    Dato un pattern come lista di token, restituisce tutti i sotto-pattern validi
    (almeno una tripla, cioè ≥ 3 token) diversi dal pattern stesso.

    Ogni sotto-pattern è restituito come lista di token.
    """
    subpatterns = []
    n = len(tokens)  # dispari

    # I nodi si trovano agli indici pari: 0, 2, 4, ...
    # Un sotto-pattern valido va da indice pari `start` a indice pari `end`
    for start in range(0, n - 2, 2):          # almeno 1 connector → end ≥ start+2
        for end in range(start + 2, n, 2):    # end è indice di un nodo
            sub = tokens[start:end + 1]       # include il nodo finale
            if len(sub) >= 3 and sub != tokens:
                subpatterns.append(sub)

    # Deduplicazione (stessa sequenza di token)
    seen = set()
    unique = []
    for sp in subpatterns:
        key = tuple(sp)
        if key not in seen:
            seen.add(key)
            unique.append(sp)

    return unique


# ---------------------------------------------------------------------------
# GRAPH BUILDING
# ---------------------------------------------------------------------------

def _node_id(token: str) -> str:
    """Chiave univoca per un token-nodo."""
    return token


def _edge_id(connector: str) -> str:
    """Chiave univoca per un token-connector (strip delle frecce)."""
    return connector_raw_token(connector)


def _parse_node_token(token: str) -> tuple[str, str]:
    """
    Da "(alias:Label)" estrae (alias, label_clean).
    Supporta label multiple tipo ":Label1:Label2" → usa la prima come label.
    """
    inner = token.strip('()')
    if ':' in inner:
        alias, rest = inner.split(':', 1)
        # label pulita: prendi tutto fino al prossimo ':' (label multiple)
        label_clean = rest.split(':')[0]
    else:
        alias = inner
        label_clean = inner
    return alias.strip(), label_clean.strip()


def _parse_edge_token(connector: str) -> tuple[str, str, str]:
    """
    Da "-[alias:Label]->" estrae (alias, label_clean, direction).
    """
    direction = connector_direction(connector)
    inner = re.search(r'\[([^\]]*)\]', connector)
    if inner:
        content = inner.group(1)
        if ':' in content:
            alias, label_clean = content.split(':', 1)
        else:
            alias = content
            label_clean = content
    else:
        alias = connector
        label_clean = connector
    return alias.strip(), label_clean.strip(), direction


def tokens_alias_index(tokens: list[str]) -> dict[str, int]:
    """Mappa alias → indice del token nel pattern (pari = nodo, dispari = arco)."""
    index = {}
    for i, tok in enumerate(tokens):
        alias = _parse_node_token(tok)[0] if i % 2 == 0 else _parse_edge_token(tok)[0]
        index.setdefault(alias, i)
    return index


def _applicable(filters, aliases) -> list:
    """Filtri (query_filters.Filter) i cui alias sono tutti contenuti in `aliases`."""
    return [f for f in filters if f.aliases <= aliases]


def build_pattern_graph(pattern: str, filters: list | None = None) -> nx.DiGraph:
    """
    Costruisce il grafo dei sotto-pattern a partire da un pattern Cypher-like.

    Supporta pattern undirected, right-directed, left-directed e misti, es.:
      (a:A)-[r:R]-(b:B)
      (a:A)-[r:R]->(b:B)<-[s:S]-(c:C)
      (a:A)<-[r:R]-(b:B)-[s:S]->(c:C)

    Struttura del grafo risultante:
      - Nodi foglia 'node' e 'edge': componenti atomici del pattern
      - Nodi 'tripla': sotto-pattern di lunghezza 3
      - Nodi 'subpattern': sotto-pattern di lunghezza ≥ 5
      - Nodo 'original_pattern': il pattern originale completo

    Ogni nodo ha gli attributi:
      label, pattern, sub_key, external_key, info_for_ext_match, visited
    I nodi foglia 'node' hanno in più: original_label, alias
    I nodi foglia 'edge' hanno in più: original_label, alias, direction

    `filters` è la lista prodotta da query_filters.parse_filters. Ogni nodo
    riceve gli attributi:
      aliases → alias contenuti nel nodo
      filters → i filtri applicabili a quel nodo (tutti i loro alias sono nel nodo)
    """
    filters = filters or []
    G = nx.DiGraph()
    visited_patterns: set[tuple] = set()

    try:
        root_tokens = tokenize_pattern(pattern)
    except ValueError as e:
        print(f"[ERROR] {e}")
        return G

    def _add_leaf_nodes(tokens: list[str]):
        """
        Aggiunge al grafo i nodi foglia (node + edge) di una tripla o sequenza.
        Ritorna la lista delle chiavi aggiunte nell'ordine [nodo, arco, nodo, ...].
        """
        keys = []
        for i, tok in enumerate(tokens):
            if i % 2 == 0:  # nodo
                nid = _node_id(tok)
                if nid not in G:
                    alias, label_clean = _parse_node_token(tok)
                    G.add_node(nid,
                               label='node',
                               pattern=tok,
                               original_label=label_clean,
                               alias=alias,
                               aliases={alias},
                               filters=_applicable(filters, {alias}),
                               external_key=False,
                               info_for_ext_match=[],
                               sub_key=[],
                               visited=False)
                keys.append(nid)
            else:  # connector
                eid = _edge_id(tok)
                if eid not in G:
                    alias, label_clean, direction = _parse_edge_token(tok)
                    G.add_node(eid,
                               label='edge',
                               pattern=eid,
                               original_label=label_clean,
                               alias=alias,
                               direction=direction,
                               aliases={alias},
                               filters=_applicable(filters, {alias}),
                               external_key=False,
                               info_for_ext_match=[],
                               sub_key=[],
                               visited=False)
                keys.append(eid)
        return keys

    def recurse(tokens: list[str]):
        """Costruisce ricorsivamente il grafo per un sotto-pattern."""
        key = tuple(tokens)
        if key in visited_patterns:
            return
        visited_patterns.add(key)

        pattern_str = tokens_to_pattern(tokens)
        n_tokens = len(tokens)

        if n_tokens == 1:
            return  # singolo nodo: ignorato

        if n_tokens == 3:
            node_label = 'tripla'
        elif n_tokens >= 5 and n_tokens % 2 == 1:
            node_label = 'subpattern'
        else:
            return

        # Aggiungi il nodo-pattern se non esiste
        if pattern_str not in G:
            aliases = set(tokens_alias_index(tokens))
            G.add_node(pattern_str,
                       label=node_label,
                       pattern=pattern_str,
                       aliases=aliases,
                       filters=_applicable(filters, aliases),
                       sub_key=[],
                       external_key=False,
                       info_for_ext_match=[],
                       visited=False)

        if n_tokens == 3:
            # Aggiungi foglie e archi pattern → foglie
            leaf_keys = _add_leaf_nodes(tokens)
            for lk in leaf_keys:
                G.add_edge(pattern_str, lk)
        else:
            # Aggiungi sotto-pattern figli
            for sub_tokens in extract_all_valid_subpatterns(tokens):
                if tuple(sub_tokens) == key:
                    continue
                recurse(sub_tokens)
                sub_str = tokens_to_pattern(sub_tokens)
                G.add_edge(pattern_str, sub_str)

    # ----------------------------------------------------------------
    # Radice
    # ----------------------------------------------------------------
    n_root = len(root_tokens)
    if n_root == 3:
        root_label = 'tripla'
    elif n_root >= 5 and n_root % 2 == 1:
        root_label = 'original_pattern'
    else:
        print(f"[WARN] Pattern non valido (lunghezza token: {n_root}): {pattern!r}")
        return G

    root_str = tokens_to_pattern(root_tokens)
    root_aliases = set(tokens_alias_index(root_tokens))
    G.add_node(root_str,
               label=root_label,
               pattern=root_str,
               aliases=root_aliases,
               filters=_applicable(filters, root_aliases),
               sub_key=[],
               external_key=False,
               info_for_ext_match=[],
               visited=False)

    if root_label == 'original_pattern':
        for sub_tokens in extract_all_valid_subpatterns(root_tokens):
            recurse(sub_tokens)
            sub_str = tokens_to_pattern(sub_tokens)
            G.add_edge(root_str, sub_str)
    else:
        # Pattern è già una tripla: aggiungi direttamente le foglie
        leaf_keys = _add_leaf_nodes(root_tokens)
        for lk in leaf_keys:
            G.add_edge(root_str, lk)

    if filters:
        from query_filters import dbg  # import locale: query_filters importa questo modulo
        tot = G.number_of_nodes()
        for k, f in enumerate(filters):
            n_nodes = sum(1 for _, d in G.nodes(data=True) if f in d["filters"])
            dbg(f"#{k} applicato a {n_nodes}/{tot} nodi dell'albero")
        for n, d in G.nodes(data=True):
            if d["filters"]:
                dbg(f"  {d['label']:<16} {n}  →  {[f.text for f in d['filters']]}", level=2)

    return G


# ---------------------------------------------------------------------------
# QUICK SELF-TEST
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    test_patterns = [
        # undirected (originale)
        "(a:Account)-[wd:Withdraw]-(a2:Account)-[tran:Transfer]-(a3:Account)",
        # fully directed →
        "(p:Patient)-[has:HAS_ENCOUNTER]->(en:Encounter)-[hasp:HAS_PROCEDURE]->(pro:Procedure)",
        # mixed ← →
        "(a:A)-[r:R]->(b:B)<-[s:S]-(c:C)",
        # left-directed
        "(a:A)<-[r:R]-(b:B)<-[s:S]-(c:C)",
        # mixed lungo
        "(os:OS:Process)-[ins:INSTANCE]->(ver:Version)<-[ins2:INSTANCE]-(os2:OS:Process)",
        # lunga con più tipi
        "(a:A)-[r1:R1]->(b:B)-[r2:R2]-(c:C)<-[r3:R3]-(d:D)",
    ]

    for pat in test_patterns:
        print(f"\n{'='*70}")
        print(f"PATTERN: {pat}")
        try:
            tokens = tokenize_pattern(pat)
            print(f"  Tokens ({len(tokens)}): {tokens}")
            subs = extract_all_valid_subpatterns(tokens)
            print(f"  Sotto-pattern estratti ({len(subs)}):")
            for s in subs:
                print(f"    {tokens_to_pattern(s)}")
            G = build_pattern_graph(pat)
            print(f"  Grafo: {G.number_of_nodes()} nodi, {G.number_of_edges()} archi")
            for n, d in G.nodes(data=True):
                if d['label'] == 'edge':
                    print(f"    [edge] {n!r}  direction={d.get('direction', '')!r}")
        except Exception as e:
            print(f"  ERRORE: {e}")
