#!/usr/bin/env python
"""
Esperimento di integrity checking / robustezza al rumore su PG-Keymaker.

Due modalità (sottocomandi):

  validate  rivalida sul database le chiavi interne trovate da PG-Keymaker sul grafo
            pulito, nodo per nodo dell'SP-Tree, con lo stesso match block (filtri
            compresi) usato dall'algoritmo. Per il primo caso violato di ogni livello
            estrae un esempio di violazione.

  compare   confronta le chiavi interne del CSV sul grafo pulito con quelle del CSV
            sul grafo sporco: rimaste / sparite / nuove, per sotto-pattern e livello.

Tutti i file di output vengono scritti nella cartella di questo script (use_case/).
Le funzioni del repository vengono importate e usate senza modifiche; le uniche
funzioni nuove (match_block_for_node, check_keys_with_block, find_violation_example)
sono definite qui sotto e motivate nei rispettivi docstring.
"""
import argparse
import ast
import csv
import json
import os
import sys
from collections import OrderedDict

# --- import dal repository (la radice è la cartella sopra use_case/) ---------
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import subpattern_extractor_v2 as sub_ext   # noqa: E402
import query_filters as qf                  # noqa: E402

qf.DEBUG_FILTRI = 0   # niente stampe [FILTRI]

# ===========================================================================
# CONFIGURAZIONE: modifica qui i parametri e lancia `python noise_experiment.py`
# (se passi argomenti da riga di comando, quelli hanno la precedenza)
# ===========================================================================
MODE = "compare"            # "validate" oppure "compare"
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "gianpaolo")
DATABASE = "finbench_10perc"  # nome dell'istanza: solo etichetta dei file di output (le query vanno sul db "neo4j")
PATTERN = ("(src1:Account)-[edge1:Transfer]->(mid:Account)-[edge2:Withdraw]->(dstCard:Account)",
           "datetime('2021-01-01T00:00:00Z') < edge1.createTime < datetime('2022-01-01T00:00:00Z') "
           "AND datetime('2021-01-01T00:00:00Z') < edge2.createTime < datetime('2022-01-01T00:00:00Z') "
           "AND edge1.amount > 50 AND edge2.amount > 100")
CLEAN_CSV = "../evaluation/scalability/pgkeymaker_hyucc/meaningfull/results_PGKEYMAKER_hyucc_finbench_101.txt"
DIRTY_CSV = "../evaluation/scalability/pgkeymaker_hyucc/meaningfull/results_PGKEYMAKER_hyucc_finbench_10perc_99.txt"               # solo per compare
# ===========================================================================



LEVELS = ("root", "intermediate", "leaf")
LEVEL_RANK = {"root": 0, "intermediate": 1, "leaf": 2}


# ---------------------------------------------------------------------------
# CSV dei risultati di PG-Keymaker
# ---------------------------------------------------------------------------

def parse_results_csv(path):
    """
    Legge un file risultati di PG-Keymaker (formato di save_results_from_graph).

    Ritorna un dict:
      filters      stringa filtri (riga 'filters: ...') oppure ""
      rows         OrderedDict  pattern -> lista di chiavi (ognuna tupla di 'alias.prop')
      external     numero di righe 'external keys from ...' (foglie con chiavi esterne:
                   il campo pattern è sostituito, quindi non sono associabili a un nodo)
    """
    filters = ""
    rows = OrderedDict()
    external = 0
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\r\n")
            if line.startswith("filters:"):
                filters = line[len("filters:"):].strip()
                continue
            if line.startswith("pattern;keys found"):
                continue
            if line.startswith("execution time") or line.startswith("Max memory used"):
                break
            if ";" not in line:
                continue
            pattern, _, keys_str = line.partition(";")
            pattern = pattern.strip()
            if "external keys from" in pattern:
                external += 1
                continue
            keys = ast.literal_eval(keys_str.strip())
            norm = []
            for k in keys:
                key = (k,) if isinstance(k, str) else tuple(k)
                if key not in norm:
                    norm.append(key)
            rows[pattern] = norm
    return {"filters": filters, "rows": rows, "external": external}


def subpattern_aliases(sub):
    """Alias contenuti in un sotto-pattern dato come stringa (nodo, arco '[r:R]' o cammino)."""
    sub = sub.strip()
    if sub.startswith("["):
        return {sub_ext._parse_edge_token(sub)[0]}
    return set(sub_ext.tokens_alias_index(sub_ext.tokenize_pattern(sub)))


def internal_keys(sub, keys):
    """Solo le chiavi i cui attributi appartengono tutti al sotto-pattern (scarta le esterne)."""
    aliases = subpattern_aliases(sub)
    return [k for k in keys if all(a.split(".", 1)[0] in aliases for a in k)]


def split_pattern_filters(p):
    """Accetta (pattern, filtri), [pattern, filtri], "pattern||filtri" o solo "pattern"."""
    if isinstance(p, (tuple, list)):
        return p[0].strip(), (p[1].strip() if len(p) > 1 else "")
    pat, _, flt = p.partition("||")
    return pat.strip(), flt.strip()


def key_str(key):
    return str(list(key))


# ---------------------------------------------------------------------------
# Funzioni nuove (le originali non si toccano)
# ---------------------------------------------------------------------------

def match_block_for_node(node, filters):
    """
    Match block (MATCH ... WHERE ..., senza RETURN) che l'algoritmo usa per il nodo.

    Perché serve: check_key_uniqueness_apoc costruisce il blocco da solo con
    qf.build_match_block(pattern, filters), e va bene per nodi e cammini; ma per una
    foglia-arco "[r:R]" darebbe "MATCH [r:R]" (Cypher non valido), mentre l'algoritmo
    la matcha come "(a)-[r:R]-(b)" con "id(a) < id(b)" (n4j.generate_query_from_pattern).
    Per le foglie-arco il blocco si ricava quindi da generate_query_from_pattern,
    togliendo il RETURN finale, senza duplicarne la logica.
    """
    if node.strip().startswith("["):
        import neo4j_querying_module as n4j
        query = n4j.generate_query_from_pattern(node, filters)
        return query[:query.rfind("RETURN ")].rstrip()
    return qf.build_match_block(node, filters)


def match_aliases(node):
    """Alias presenti nello scope del match block di un nodo (per gli elementId negli esempi)."""
    node = node.strip()
    if node.startswith("["):
        return ["a", "b", sub_ext._parse_edge_token(node)[0]]   # (a)-[r:R]-(b), come nell'algoritmo
    return list(sub_ext.tokens_alias_index(sub_ext.tokenize_pattern(node)))


def check_keys_with_block(match_block, key_sets, driver):
    """
    Stessa query APOC di n4j.check_key_uniqueness_apoc, ma con match block già pronto.

    Perché serve: l'originale ricostruisce il blocco dal pattern e non accetta extra_where,
    quindi non può validare le foglie-arco con il blocco usato dall'algoritmo.
    Ritorna (risultati, n_righe) come l'originale.
    """
    query = """
    UNWIND $key_sets AS key_attrs
    WITH key_attrs,
         [x IN key_attrs | x + " AS " + replace(x, ".", "_")] AS aliased,
         [x IN key_attrs | replace(x, ".", "_")] AS alias_names
    WITH key_attrs,
         $match_block +
         " WITH " + apoc.text.join(aliased, ", ") +
         " RETURN count(*) AS n, count(*) > 0 AND count(*) = count(DISTINCT " +
         CASE
             WHEN size(alias_names) > 1
             THEN "[" + apoc.text.join(alias_names, ", ") + "]"
             ELSE head(alias_names)
         END +
         ") AND count(*) = count(CASE WHEN all(v IN [" + apoc.text.join(alias_names, ", ") +
         "] WHERE v IS NOT NULL) THEN 1 END) AS is_unique" AS q
    CALL apoc.cypher.run(q, {}) YIELD value
    RETURN key_attrs, value.is_unique AS is_unique, value.n AS n
    """
    with driver.session() as session:
        rows = list(session.run(query, {"key_sets": [list(k) for k in key_sets],
                                        "match_block": match_block}))
    results = {tuple(r["key_attrs"]): r["is_unique"] for r in rows}
    return results, (rows[0]["n"] if rows else None)


def _jsonable(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    return str(v)


def _parse_ids(ids):
    out = {}
    for s in ids:
        alias, _, eid = s.partition("=")
        out[alias] = eid
    return out


def find_violation_example(match_block, aliases, key, driver):
    """
    Esempio di violazione di una chiave (check_key_uniqueness_apoc restituisce solo booleani).

    Stesso match block della validazione. Ritorna:
      {"type": "null_value", ...}  un match con un attributo della chiave nullo, oppure
      {"type": "duplicate", ...}   due match distinti con gli stessi valori della chiave
    con valori della chiave ed elementId di tutti gli alias dei match.
    Scelta deterministica (ORDER BY), nessuna casualità. None se non trova violazioni.
    """
    ids_expr = ", ".join(f'"{a}=" + elementId({a})' for a in aliases)
    key_expr = ", ".join(key)
    head = f"{match_block}\nWITH [{ids_expr}] AS ids, [{key_expr}] AS kv\n"

    q_null = head + ("WITH ids, kv WHERE any(v IN kv WHERE v IS NULL)\n"
                     "RETURN ids, kv ORDER BY ids LIMIT 1")
    q_dup = head + ("WITH ids, kv WHERE none(v IN kv WHERE v IS NULL)\n"
                    "WITH kv, collect(ids) AS ms WHERE size(ms) > 1\n"
                    "WITH kv, ms ORDER BY toString(kv) LIMIT 1\n"
                    "UNWIND ms AS m WITH kv, m ORDER BY m LIMIT 2\n"
                    "RETURN kv, collect(m) AS pair")
    with driver.session() as session:
        r = session.run(q_null).single()
        if r:
            return {"type": "null_value", "key": list(key),
                    "key_values": [_jsonable(v) for v in r["kv"]],
                    "matches": [_parse_ids(r["ids"])]}
        r = session.run(q_dup).single()
        if r:
            return {"type": "duplicate", "key": list(key),
                    "key_values": [_jsonable(v) for v in r["kv"]],
                    "matches": [_parse_ids(m) for m in r["pair"]]}
    return None


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_summary(database, mode, section):
    """Scrive summary_<database>.json; conserva la sezione dell'altra modalità se già presente."""
    path = os.path.join(HERE, f"summary_{database}.json")
    data = {}
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            data = {}
    data["database"] = database
    data[mode] = section
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True, default=str)
    return path


def write_details(database, mode, header, rows):
    path = os.path.join(HERE, f"details_{database}_{mode}.csv")
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return path


# ---------------------------------------------------------------------------
# Modalità validate
# ---------------------------------------------------------------------------

def run_validate(args):
    from neo4j import GraphDatabase
    import neo4j_querying_module as n4j

    clean = parse_results_csv(args.clean_csv)
    warnings = []

    pattern_in, filters_str = split_pattern_filters(args.pattern)
    if clean["filters"].strip() != filters_str.strip():
        warnings.append(f"i filtri dati non coincidono con quelli del CSV: "
                        f"dati={filters_str!r} csv={clean['filters']!r}")

    pattern = sub_ext.tokens_to_pattern(sub_ext.tokenize_pattern(pattern_in))
    filters = qf.parse_filters(filters_str, pattern)
    G = sub_ext.build_pattern_graph(pattern, filters)
    if G.number_of_nodes() == 0:
        sys.exit(f"SP-Tree vuoto per il pattern {args.pattern!r}")

    def level_of(n):
        if G.nodes[n]["label"] in ("node", "edge"):
            return "leaf"
        return "root" if G.in_degree(n) == 0 else "intermediate"

    nodes = sorted(G.nodes, key=lambda n: (LEVEL_RANK[level_of(n)], n))

    # confronto tra nodi dell'albero e righe del CSV
    rows = clean["rows"]
    missing_in_csv = [n for n in nodes if n not in rows]
    missing_in_tree = [p for p in rows if p not in G.nodes]
    if missing_in_csv:
        note = " (possibili foglie con chiavi esterne, riga sostituita nel CSV)" if clean["external"] else ""
        warnings.append(f"sotto-pattern dell'albero assenti dal CSV{note}: {missing_in_csv}")
    if missing_in_tree:
        warnings.append(f"righe del CSV assenti dall'albero: {missing_in_tree}")
    if clean["external"]:
        warnings.append(f"{clean['external']} righe 'external keys from ...' ignorate")

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))

    detail_rows = []
    totals = {l: {"total": 0, "violated": 0} for l in LEVELS}
    examples = {}
    try:
        for n in nodes:
            level = level_of(n)
            keys = internal_keys(n, rows.get(n, []))
            if not keys:
                continue
            nfilters = G.nodes[n].get("filters", [])
            if n.startswith("["):
                results, _ = check_keys_with_block(match_block_for_node(n, nfilters), keys, driver)
            else:
                results, _ = n4j.check_key_uniqueness_apoc(n, [list(k) for k in keys], driver, nfilters)
            first_violated = None
            for k in keys:
                holds = bool(results.get(tuple(k), False))
                totals[level]["total"] += 1
                if not holds:
                    totals[level]["violated"] += 1
                    if first_violated is None:
                        first_violated = k
                detail_rows.append([n, level, key_str(k), holds])
            if first_violated is not None and level not in examples:
                ex = find_violation_example(match_block_for_node(n, nfilters), match_aliases(n),
                                            first_violated, driver)
                if ex is None:
                    ex = {"type": "not_reproduced", "key": list(first_violated)}
                ex["subpattern"] = n
                examples[level] = ex
                print(f"[info] esempio di violazione ({level}): {n} {key_str(first_violated)}")
    finally:
        driver.close()

    section = {
        "pattern": pattern,
        "filters": filters_str,
        "clean_csv": os.path.basename(args.clean_csv),
        "total_keys": sum(t["total"] for t in totals.values()),
        "violated_keys": sum(t["violated"] for t in totals.values()),
        "by_level": totals,
        "violation_examples": examples,
        "warnings": warnings,
    }
    detail_rows.sort(key=lambda r: (LEVEL_RANK[r[1]], r[0]))   # stabile: mantiene l'ordine delle chiavi
    print("summary:", write_summary(args.database, "validate", section))
    print("details:", write_details(args.database, "validate",
                                    ["subpattern", "level", "key", "holds"], detail_rows))
    print(f"chiavi validate: {section['total_keys']}, violate: {section['violated_keys']}")


# ---------------------------------------------------------------------------
# Modalità compare
# ---------------------------------------------------------------------------

def levels_from_patterns(patterns):
    """Livello di ogni sotto-pattern dedotto dai soli pattern (1 token = leaf, più lungo = root)."""
    size = {}
    for p in patterns:
        size[p] = 1 if p.strip().startswith("[") else len(sub_ext.tokenize_pattern(p))
    root = max(size, key=lambda p: (size[p], p)) if size else None
    return {p: ("leaf" if size[p] == 1 else "root" if p == root else "intermediate") for p in size}


def run_compare(args):
    clean = parse_results_csv(args.clean_csv)
    dirty = parse_results_csv(args.dirty_csv)
    warnings = []
    if clean["filters"].strip() != dirty["filters"].strip():
        warnings.append(f"filtri diversi tra i due CSV: pulito={clean['filters']!r} sporco={dirty['filters']!r}")
    for name, d in (("pulito", clean), ("sporco", dirty)):
        if d["external"]:
            warnings.append(f"{d['external']} righe 'external keys from ...' ignorate nel CSV {name}")

    patterns = list(OrderedDict.fromkeys(list(clean["rows"]) + list(dirty["rows"])))
    only_clean = [p for p in patterns if p not in dirty["rows"]]
    only_dirty = [p for p in patterns if p not in clean["rows"]]
    if only_clean:
        warnings.append(f"sotto-pattern presenti solo nel CSV pulito: {only_clean}")
    if only_dirty:
        warnings.append(f"sotto-pattern presenti solo nel CSV sporco: {only_dirty}")

    levels = levels_from_patterns(patterns)
    totals = {l: {"kept": 0, "lost": 0, "new": 0} for l in LEVELS}
    detail_rows = []

    def norm(sub, keys):
        out = OrderedDict()
        for k in internal_keys(sub, keys):
            out[frozenset(k)] = tuple(sorted(k))
        return out

    for p in sorted(patterns, key=lambda p: (LEVEL_RANK[levels[p]], p)):
        c = norm(p, clean["rows"].get(p, []))
        d = norm(p, dirty["rows"].get(p, []))
        for fs in sorted(c, key=lambda f: c[f]):
            status = "kept" if fs in d else "lost"
            totals[levels[p]][status] += 1
            detail_rows.append([p, levels[p], key_str(c[fs]), status])
        for fs in sorted(d, key=lambda f: d[f]):
            if fs not in c:
                totals[levels[p]]["new"] += 1
                detail_rows.append([p, levels[p], key_str(d[fs]), "new"])

    section = {
        "clean_csv": os.path.basename(args.clean_csv),
        "dirty_csv": os.path.basename(args.dirty_csv),
        "kept": sum(t["kept"] for t in totals.values()),
        "lost": sum(t["lost"] for t in totals.values()),
        "new": sum(t["new"] for t in totals.values()),
        "by_level": totals,
        "warnings": warnings,
    }
    print("summary:", write_summary(args.database, "compare", section))
    print("details:", write_details(args.database, "compare",
                                    ["subpattern", "level", "key", "status"], detail_rows))
    print(f"rimaste: {section['kept']}, sparite: {section['lost']}, nuove: {section['new']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) == 1:   # nessun argomento: usa la configurazione in cima al file
        from types import SimpleNamespace
        args = SimpleNamespace(uri=URI, user=AUTH[0], password=AUTH[1], database=DATABASE,
                               pattern=PATTERN, clean_csv=CLEAN_CSV,
                               dirty_csv=DIRTY_CSV)
        {"validate": run_validate, "compare": run_compare}[MODE](args)
        return
    ap = argparse.ArgumentParser(description="Integrity checking / robustezza al rumore per PG-Keymaker")
    sub = ap.add_subparsers(dest="mode", required=True)

    v = sub.add_parser("validate", help="valida sul database le chiavi trovate sul grafo pulito")
    v.add_argument("--uri", default="neo4j://localhost:7687")
    v.add_argument("--user", default="neo4j")
    v.add_argument("--password", default=AUTH[1])
    v.add_argument("--database", required=True, help="nome dell'istanza: solo etichetta dei file di output (le query vanno sul db di default)")
    v.add_argument("--pattern", required=True,
                   help='come per PG-Keymaker: "pattern" oppure "pattern||filtri"')
    v.add_argument("--clean-csv", required=True, help="risultati di PG-Keymaker sul grafo pulito")
    v.set_defaults(func=run_validate)

    c = sub.add_parser("compare", help="confronta le chiavi del grafo pulito con quelle del grafo sporco")
    c.add_argument("--database", required=True, help="etichetta usata nei nomi dei file (es. il db sporco)")
    c.add_argument("--clean-csv", required=True)
    c.add_argument("--dirty-csv", required=True)
    c.set_defaults(func=run_compare)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
