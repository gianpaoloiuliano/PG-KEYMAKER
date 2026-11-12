#!/usr/bin/env python
"""
Integrity checking / noise robustness experiment on PG-Keymaker.

Two modes (subcommands):

  validate  re-validates on the database the internal keys found by PG-Keymaker on the
            clean graph, node by node of the SP-Tree, with the same match block (filters
            included) used by the algorithm. For the first violated case of each level
            it extracts an example of violation.

  compare   compares the internal keys of the CSV on the clean graph with those of the CSV
            on the dirty graph: kept / lost / new, per sub-pattern and level.

All the output files are written to the folder of this script (use_case/).
The repository functions are imported and used unchanged; the only new
functions (match_block_for_node, check_keys_with_block, find_violation_example)
are defined below and motivated in their respective docstrings.
"""
import argparse
import ast
import csv
import json
import os
import sys
from collections import OrderedDict

# --- imports from the repository (the root is the folder above use_case/) ---------
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import subpattern_extractor_v2 as sub_ext   # noqa: E402
import query_filters as qf                  # noqa: E402

qf.DEBUG_FILTRI = 0   # no [FILTERS] prints

# ===========================================================================
# CONFIGURATION: edit the parameters here and run `python noise_experiment.py`
# (if you pass command-line arguments, those take precedence)
# ===========================================================================
MODE = "compare"            # "validate" or "compare"
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "gianpaolo")
DATABASE = "finbench_10perc"  # instance name: only a label for the output files (queries go to the "neo4j" db)
PATTERN = ("(src1:Account)-[edge1:Transfer]->(mid:Account)-[edge2:Withdraw]->(dstCard:Account)",
           "datetime('2021-01-01T00:00:00Z') < edge1.createTime < datetime('2022-01-01T00:00:00Z') "
           "AND datetime('2021-01-01T00:00:00Z') < edge2.createTime < datetime('2022-01-01T00:00:00Z') "
           "AND edge1.amount > 50 AND edge2.amount > 100")
CLEAN_CSV = "../evaluation/scalability/pgkeymaker_hyucc/meaningfull/results_PGKEYMAKER_hyucc_finbench_101.txt"
DIRTY_CSV = "../evaluation/scalability/pgkeymaker_hyucc/meaningfull/results_PGKEYMAKER_hyucc_finbench_10perc_99.txt"               # compare only
# ===========================================================================



LEVELS = ("root", "intermediate", "leaf")
LEVEL_RANK = {"root": 0, "intermediate": 1, "leaf": 2}


# ---------------------------------------------------------------------------
# PG-Keymaker results CSV
# ---------------------------------------------------------------------------

def parse_results_csv(path):
    """
    Reads a PG-Keymaker results file (save_results_from_graph format).

    Returns a dict:
      filters      filter string ('filters: ...' line) or ""
      rows         OrderedDict  pattern -> list of keys (each a tuple of 'alias.prop')
      external     number of 'external keys from ...' lines (leaves with external keys:
                   the pattern field is replaced, so they cannot be associated with a node)
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
    """Aliases contained in a sub-pattern given as a string (node, edge '[r:R]' or path)."""
    sub = sub.strip()
    if sub.startswith("["):
        return {sub_ext._parse_edge_token(sub)[0]}
    return set(sub_ext.tokens_alias_index(sub_ext.tokenize_pattern(sub)))


def internal_keys(sub, keys):
    """Only the keys whose attributes all belong to the sub-pattern (discards the external ones)."""
    aliases = subpattern_aliases(sub)
    return [k for k in keys if all(a.split(".", 1)[0] in aliases for a in k)]


def split_pattern_filters(p):
    """Accepts (pattern, filters), [pattern, filters], "pattern||filters" or just "pattern"."""
    if isinstance(p, (tuple, list)):
        return p[0].strip(), (p[1].strip() if len(p) > 1 else "")
    pat, _, flt = p.partition("||")
    return pat.strip(), flt.strip()


def key_str(key):
    return str(list(key))


# ---------------------------------------------------------------------------
# New functions (the original ones are left untouched)
# ---------------------------------------------------------------------------

def match_block_for_node(node, filters):
    """
    Match block (MATCH ... WHERE ..., without RETURN) that the algorithm uses for the node.

    Why it is needed: check_key_uniqueness_apoc builds the block by itself with
    qf.build_match_block(pattern, filters), which is fine for nodes and paths; but for an
    edge leaf "[r:R]" it would give "MATCH [r:R]" (invalid Cypher), whereas the algorithm
    matches it as "(a)-[r:R]-(b)" with "id(a) < id(b)" (n4j.generate_query_from_pattern).
    For edge leaves the block is therefore derived from generate_query_from_pattern,
    removing the final RETURN, without duplicating its logic.
    """
    if node.strip().startswith("["):
        import neo4j_querying_module as n4j
        query = n4j.generate_query_from_pattern(node, filters)
        return query[:query.rfind("RETURN ")].rstrip()
    return qf.build_match_block(node, filters)


def match_aliases(node):
    """Aliases present in the scope of a node's match block (for the elementIds in the examples)."""
    node = node.strip()
    if node.startswith("["):
        return ["a", "b", sub_ext._parse_edge_token(node)[0]]   # (a)-[r:R]-(b), as in the algorithm
    return list(sub_ext.tokens_alias_index(sub_ext.tokenize_pattern(node)))


def check_keys_with_block(match_block, key_sets, driver):
    """
    Same APOC query as n4j.check_key_uniqueness_apoc, but with a ready-made match block.

    Why it is needed: the original rebuilds the block from the pattern and does not accept extra_where,
    so it cannot validate edge leaves with the block used by the algorithm.
    Returns (results, n_rows) like the original.
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
    Example of a key violation (check_key_uniqueness_apoc returns only booleans).

    Same match block as the validation. Returns:
      {"type": "null_value", ...}  a match with a null key attribute, or
      {"type": "duplicate", ...}   two distinct matches with the same key values
    with the key values and the elementId of all the aliases of the matches.
    Deterministic choice (ORDER BY), no randomness. None if no violations are found.
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
    """Writes summary_<database>.json; keeps the other mode's section if already present."""
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
# Validate mode
# ---------------------------------------------------------------------------

def run_validate(args):
    from neo4j import GraphDatabase
    import neo4j_querying_module as n4j

    clean = parse_results_csv(args.clean_csv)
    warnings = []

    pattern_in, filters_str = split_pattern_filters(args.pattern)
    if clean["filters"].strip() != filters_str.strip():
        warnings.append(f"the given filters do not match those of the CSV: "
                        f"given={filters_str!r} csv={clean['filters']!r}")

    pattern = sub_ext.tokens_to_pattern(sub_ext.tokenize_pattern(pattern_in))
    filters = qf.parse_filters(filters_str, pattern)
    G = sub_ext.build_pattern_graph(pattern, filters)
    if G.number_of_nodes() == 0:
        sys.exit(f"Empty SP-Tree for the pattern {args.pattern!r}")

    def level_of(n):
        if G.nodes[n]["label"] in ("node", "edge"):
            return "leaf"
        return "root" if G.in_degree(n) == 0 else "intermediate"

    nodes = sorted(G.nodes, key=lambda n: (LEVEL_RANK[level_of(n)], n))

    # comparison between tree nodes and CSV rows
    rows = clean["rows"]
    missing_in_csv = [n for n in nodes if n not in rows]
    missing_in_tree = [p for p in rows if p not in G.nodes]
    if missing_in_csv:
        note = " (possible leaves with external keys, row replaced in the CSV)" if clean["external"] else ""
        warnings.append(f"tree sub-patterns missing from the CSV{note}: {missing_in_csv}")
    if missing_in_tree:
        warnings.append(f"CSV rows missing from the tree: {missing_in_tree}")
    if clean["external"]:
        warnings.append(f"{clean['external']} 'external keys from ...' rows ignored")

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
                print(f"[info] violation example ({level}): {n} {key_str(first_violated)}")
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
    detail_rows.sort(key=lambda r: (LEVEL_RANK[r[1]], r[0]))   # stable: keeps the order of the keys
    print("summary:", write_summary(args.database, "validate", section))
    print("details:", write_details(args.database, "validate",
                                    ["subpattern", "level", "key", "holds"], detail_rows))
    print(f"keys validated: {section['total_keys']}, violated: {section['violated_keys']}")


# ---------------------------------------------------------------------------
# Compare mode
# ---------------------------------------------------------------------------

def levels_from_patterns(patterns):
    """Level of each sub-pattern deduced from the patterns alone (1 token = leaf, longest = root)."""
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
        warnings.append(f"different filters between the two CSVs: clean={clean['filters']!r} dirty={dirty['filters']!r}")
    for name, d in (("clean", clean), ("dirty", dirty)):
        if d["external"]:
            warnings.append(f"{d['external']} 'external keys from ...' rows ignored in the {name} CSV")

    patterns = list(OrderedDict.fromkeys(list(clean["rows"]) + list(dirty["rows"])))
    only_clean = [p for p in patterns if p not in dirty["rows"]]
    only_dirty = [p for p in patterns if p not in clean["rows"]]
    if only_clean:
        warnings.append(f"sub-patterns present only in the clean CSV: {only_clean}")
    if only_dirty:
        warnings.append(f"sub-patterns present only in the dirty CSV: {only_dirty}")

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
    print(f"kept: {section['kept']}, lost: {section['lost']}, new: {section['new']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) == 1:   # no arguments: use the configuration at the top of the file
        from types import SimpleNamespace
        args = SimpleNamespace(uri=URI, user=AUTH[0], password=AUTH[1], database=DATABASE,
                               pattern=PATTERN, clean_csv=CLEAN_CSV,
                               dirty_csv=DIRTY_CSV)
        {"validate": run_validate, "compare": run_compare}[MODE](args)
        return
    ap = argparse.ArgumentParser(description="Integrity checking / noise robustness for PG-Keymaker")
    sub = ap.add_subparsers(dest="mode", required=True)

    v = sub.add_parser("validate", help="validates on the database the keys found on the clean graph")
    v.add_argument("--uri", default="neo4j://localhost:7687")
    v.add_argument("--user", default="neo4j")
    v.add_argument("--password", default=AUTH[1])
    v.add_argument("--database", required=True, help="instance name: only a label for the output files (queries go to the default db)")
    v.add_argument("--pattern", required=True,
                   help='as for PG-Keymaker: "pattern" or "pattern||filters"')
    v.add_argument("--clean-csv", required=True, help="PG-Keymaker results on the clean graph")
    v.set_defaults(func=run_validate)

    c = sub.add_parser("compare", help="compares the keys of the clean graph with those of the dirty graph")
    c.add_argument("--database", required=True, help="label used in the file names (e.g. the dirty db)")
    c.add_argument("--clean-csv", required=True)
    c.add_argument("--dirty-csv", required=True)
    c.set_defaults(func=run_compare)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
