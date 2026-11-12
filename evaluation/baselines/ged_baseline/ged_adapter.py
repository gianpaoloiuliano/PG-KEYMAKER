"""
ged_adapter.py
==============
Orchestration of the GED baseline (adapted DisGEDs) for PG-Key discovery
on a given scope.

Pipeline:
  1. parse the scope                         (parser.parse_scope, GFD baseline)
  2. sub-patterns as in HyUCC/HPIValid: single nodes, single edges and
     contiguous node-node paths (subpattern_enum.enumerate_subpatterns + edges)
  3. for each sub-pattern:
       a. matches as a DataFrame from Neo4j  (neo4j_connector, GFD baseline)
       b. internal minimal keys              (ged_baseline.find_minimal_keys)
       c. if single node without internal keys: external keys with the same
          strategy as HyUCC/HPIValid (neighbors, coverage, one-to-one)
       d. time and memory measurement (RSS in MB, as in HyUCC/HPIValid)
  4. text output in the GFD baseline format:
       FOR <pattern> IDENTIFIER <attributes>
"""

import os
import sys
import time
from dataclasses import dataclass, field
from typing import FrozenSet, List, Optional, Tuple

import pandas as pd
import psutil

# the dependencies (parser, enumeration, connector) are those of the GFD baseline,
# reused via import without modifying them; append so that local modules keep priority
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "gfd_adapted_baseline")))
from parser import parse_scope, Relationship                      # noqa: E402
from subpattern_enum import enumerate_subpatterns, SubPattern     # noqa: E402
from neo4j_connector import Neo4jConnector, qf, _hashable         # noqa: E402

from ged_baseline import find_minimal_keys                        # noqa: E402


@dataclass
class ExternalKey:
    neighbor_label: str
    rel_type: str
    keys: List[FrozenSet[str]]


@dataclass
class SubPatternResult:
    pattern: str
    n_matches: int
    keys: List[FrozenSet[str]] = field(default_factory=list)
    external_keys: List[ExternalKey] = field(default_factory=list)
    elapsed_s: float = 0.0
    peak_mb: Optional[float] = None
    status: str = "ok"          # 'ok' | 'no_matches' | 'no_key'


def _rss_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)


def _format_key(key: FrozenSet[str]) -> str:
    """Renders an IDENTIFIER as a sorted list of 'var.prop' attributes."""
    return ", ".join(sorted(key))


# ----------------------------------------------------------------------------
# Sub-patterns
# ----------------------------------------------------------------------------

def _edge_display(rel: Relationship) -> str:
    return f"[{rel.var}:{rel.rel_type}]"


def _flatten(records, columns: List[str]) -> pd.DataFrame:
    return pd.DataFrame(records, columns=columns)


def match_edge(connector: Neo4jConnector, rel: Relationship, filters=None
               ) -> Tuple[pd.DataFrame, int]:
    """
    Match of a single edge: one row per relationship, columns 'var.prop'.
    Undirected edges are returned by Cypher in both orientations: only one
    orientation is kept, with id(s) <= id(t).
    """
    props = connector.rel_properties(rel.rel_type)
    if rel.direction == "right":
        pattern, extra = f"(s)-[{rel.var}:{rel.rel_type}]->(t)", None
    elif rel.direction == "left":
        pattern, extra = f"(s)<-[{rel.var}:{rel.rel_type}]-(t)", None
    else:
        pattern, extra = f"(s)-[{rel.var}:{rel.rel_type}]-(t)", "id(s) <= id(t)"

    match_block = qf.build_match_block(pattern, filters, extra)
    if not props:
        query = f"{match_block}\nRETURN count(*) AS support"
        with connector._driver.session() as session:
            rec = session.run(query).single()
        support = int(rec["support"]) if rec else 0
        return pd.DataFrame(index=range(support)), support

    returns = ", ".join(f"{rel.var}.`{p}` AS col{i}" for i, p in enumerate(props))
    query = f"{match_block}\nRETURN {returns}"
    with connector._driver.session() as session:
        records = [[_hashable(rec[f"col{i}"]) for i in range(len(props))]
                   for rec in session.run(query)]
    df = _flatten(records, [f"{rel.var}.{p}" for p in props])
    return df, len(df)


# ----------------------------------------------------------------------------
# External keys (same strategy as hyucc_baseline.py / hpivalid_baseline.py)
# ----------------------------------------------------------------------------

def find_neighbors(session, target_label: str) -> List[Tuple[str, str]]:
    query = f"""
    MATCH (t{target_label})-[r]-(n)
    RETURN DISTINCT labels(n)[0] AS neighborLabel, type(r) AS relType
    """
    return [(r["neighborLabel"], r["relType"]) for r in session.run(query)]


def build_neighbor_df(session, target_label: str, neighbor_label: str, rel_type: str
                      ) -> pd.DataFrame:
    query = f"""
    MATCH (t{target_label})-[:{rel_type}]-(n:{neighbor_label})
    RETURN n
    """
    rows = []
    for rec in session.run(query):
        rows.append({f"{neighbor_label}.{k}": _hashable(v)
                     for k, v in rec["n"]._properties.items()})
    return pd.DataFrame(rows)


def check_coverage(session, target_label: str, neighbor_label: str, rel_type: str) -> bool:
    query = f"""
    MATCH (t{target_label})
    OPTIONAL MATCH (t)-[:{rel_type}]-(n:{neighbor_label})
    WITH count(DISTINCT t) AS total,
         count(DISTINCT CASE WHEN n IS NOT NULL THEN t END) AS covered
    RETURN covered = total AS ok
    """
    return session.run(query).single()["ok"]


def check_one_to_one(session, target_label: str, neighbor_label: str, rel_type: str) -> bool:
    query = f"""
    MATCH (t{target_label})-[:{rel_type}]-(n:{neighbor_label})
    WITH n, count(DISTINCT t) AS c
    WHERE c > 1
    RETURN count(n) = 0 AS ok
    """
    return session.run(query).single()["ok"]


def discover_external_keys(connector: Neo4jConnector, target_label: str,
                           on_level=None) -> List[ExternalKey]:
    """target_label is the Cypher label string, e.g. ':Application:Process'."""
    found: List[ExternalKey] = []
    with connector._driver.session() as session:
        for neighbor_label, rel_type in find_neighbors(session, target_label):
            df_n = build_neighbor_df(session, target_label, neighbor_label, rel_type)
            if df_n.shape[0] < 2 or df_n.shape[1] < 2:
                continue
            keys = find_minimal_keys(df_n, on_level)
            if not keys:
                continue
            if not check_coverage(session, target_label, neighbor_label, rel_type):
                continue
            if not check_one_to_one(session, target_label, neighbor_label, rel_type):
                continue
            found.append(ExternalKey(neighbor_label, rel_type, keys))
    return found


# ----------------------------------------------------------------------------
# Discovery
# ----------------------------------------------------------------------------

def run_discovery(
    connector: Neo4jConnector,
    scope_str: str,
    track_memory: bool = True,
    filters_str: str = "",
) -> List[SubPatternResult]:
    """
    Runs the discovery on all sub-patterns of the scope and returns the results.
    `filters_str`: Cypher filters (also aggregates with PER), as in PG-KEYMAKER;
    only the filters whose aliases appear in a sub-pattern are applied to it.
    """
    scope = parse_scope(scope_str)
    paths: List[SubPattern] = enumerate_subpatterns(scope)
    nodes = [sp for sp in paths if sp.size() == 1]
    longer = [sp for sp in paths if sp.size() > 1]
    if filters_str:
        qf.dbg(f"filters: {filters_str}")
    filters = qf.parse_filters(filters_str, scope_str)

    # same order as baseline_subpatterns: nodes, edges, paths
    work: List[Tuple[str, object, List[str]]] = (
        [(sp.display(), sp, sp.variables()) for sp in nodes]
        + [(_edge_display(r), r, [r.var]) for r in scope.relationships]
        + [(sp.display(), sp, sp.variables()) for sp in longer]
    )

    results: List[SubPatternResult] = []
    peak = _rss_mb()

    def sample():
        nonlocal peak
        if track_memory:
            peak = max(peak, _rss_mb())

    for pattern_str, item, variables in work:
        sp_filters = qf.applicable_filters(filters, variables)
        if filters:
            print(f"sub-pattern {pattern_str}{qf.filters_label(sp_filters)}")

        peak = _rss_mb()
        t0 = time.perf_counter()

        if isinstance(item, Relationship):
            df, n_matches = match_edge(connector, item, sp_filters)
        else:
            df, _, n_matches = connector.match_subpattern(item, sp_filters)
        sample()

        res = SubPatternResult(pattern=pattern_str, n_matches=n_matches)

        if n_matches == 0:
            res.status = "no_matches"
        else:
            res.keys = find_minimal_keys(df, sample)
            # external keys: only single node without internal keys (as in HyUCC)
            if not res.keys and isinstance(item, SubPattern) and item.size() == 1 \
                    and df.shape[0] >= 2 and df.shape[1] >= 2:
                res.external_keys = discover_external_keys(
                    connector, item.nodes[0].cypher_label_str(), sample)
            res.status = "ok" if (res.keys or res.external_keys) else "no_key"

        sample()
        res.elapsed_s = time.perf_counter() - t0
        if track_memory:
            res.peak_mb = peak

        results.append(res)

    return results


def render_report(results: List[SubPatternResult]) -> str:
    """Builds the complete text report (also for saving to a file)."""
    lines: List[str] = []
    total_keys = 0
    total_time = 0.0
    max_peak = 0.0

    lines.append("=" * 72)
    lines.append("GED-ADAPTED BASELINE (DisGEDs) — PG-Key discovery")
    lines.append("=" * 72)

    for res in results:
        total_time += res.elapsed_s
        if res.peak_mb is not None:
            max_peak = max(max_peak, res.peak_mb)

        lines.append("")
        lines.append(f"FOR {res.pattern}")
        mem = f", peak {res.peak_mb:.1f} MB" if res.peak_mb is not None else ""
        lines.append(f"  match: {res.n_matches} | time: {res.elapsed_s*1000:.2f} ms{mem}")

        if res.status == "no_matches":
            lines.append("  [skip] no match for this sub-pattern")
            continue
        if res.status == "no_key":
            lines.append("  no key found")
            continue

        for key in res.keys:
            total_keys += 1
            lines.append(f"  IDENTIFIER {_format_key(key)}")
        for ext in res.external_keys:
            for key in ext.keys:
                total_keys += 1
                lines.append(
                    f"  EXTERNAL IDENTIFIER {_format_key(key)} VIA {ext.rel_type}")

    lines.append("")
    lines.append("-" * 72)
    lines.append(f"Total PG-Keys found    : {total_keys}")
    lines.append(f"Sub-patterns examined  : {len(results)}")
    lines.append(f"total execution time: {total_time:.2f}s")

    if max_peak > 0:
        lines.append(f"max total memory: {max_peak:.2f} MB")
    lines.append("-" * 72)

    return "\n".join(lines)
