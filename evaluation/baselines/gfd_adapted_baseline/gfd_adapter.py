"""
gfd_adapter.py
==============
Orchestration of the whole GFD-adapted baseline for discovering internal
(internally-described) PG-Keys.

Pipeline:
  1. parse the scope                         (parser.parse_scope)
  2. enumerate contiguous sub-patterns       (subpattern_enum.enumerate_subpatterns)
  3. for each sub-pattern:
       a. property discovery + match extraction from Neo4j (neo4j_connector)
       b. UCC discovery with anti-monotonic pruning        (ucc_discovery)
       c. time and (optional) memory measurement
  4. text output grouped by sub-pattern, in the format
       FOR <pattern> IDENTIFIER <attributes>
"""

import time
import tracemalloc
from dataclasses import dataclass, field
from typing import List, Optional, FrozenSet

from parser import parse_scope
from subpattern_enum import enumerate_subpatterns, SubPattern
from neo4j_connector import Neo4jConnector, qf
from ucc_discovery import find_minimal_identifiers


@dataclass
class SubPatternResult:
    pattern: str
    n_matches: int
    keys: List[FrozenSet[str]] = field(default_factory=list)
    elapsed_s: float = 0.0
    peak_kib: Optional[float] = None
    status: str = "ok"          # 'ok' | 'no_matches' | 'below_support' | 'no_key'
    query: str = ""


def _format_key(key: FrozenSet[str]) -> str:
    """Renders an IDENTIFIER as a sorted list of 'var.prop' attributes."""
    return ", ".join(sorted(key))


def run_discovery(
    connector: Neo4jConnector,
    scope_str: str,
    min_support: int = 1,
    track_memory: bool = True,
    filters_str: str = "",
) -> List[SubPatternResult]:
    """
    Runs the discovery on all sub-patterns of the scope and returns the results.
    `filters_str`: Cypher filters (also aggregates with PER), as in PG-KEYMAKER;
    only the filters whose aliases appear in a sub-pattern are applied to it.
    """
    scope = parse_scope(scope_str)
    subpatterns: List[SubPattern] = enumerate_subpatterns(scope)
    if filters_str:
        qf.dbg(f"filters: {filters_str}")
    filters = qf.parse_filters(filters_str, scope_str)

    if track_memory and not tracemalloc.is_tracing():
        tracemalloc.start()

    results: List[SubPatternResult] = []

    for sp in subpatterns:
        pattern_str = sp.display()
        sp_filters = qf.applicable_filters(filters, sp.variables())
        if filters:
            print(f"sub-pattern {pattern_str}{qf.filters_label(sp_filters)}")

        if track_memory:
            tracemalloc.reset_peak()
        t0 = time.perf_counter()

        df, query, n_matches = connector.match_subpattern(sp, sp_filters)

        res = SubPatternResult(pattern=pattern_str, n_matches=n_matches, query=query)

        if n_matches == 0:
            res.status = "no_matches"
        elif n_matches < min_support:
            res.status = "below_support"
        else:
            keys = find_minimal_identifiers(df)
            res.keys = keys
            res.status = "ok" if keys else "no_key"

        res.elapsed_s = time.perf_counter() - t0
        if track_memory:
            _, peak = tracemalloc.get_traced_memory()
            res.peak_kib = peak / 1024.0

        results.append(res)

    if track_memory:
        tracemalloc.stop()

    return results


def render_report(results: List[SubPatternResult], min_support: int) -> str:
    """Builds the complete text report (also for saving to a file)."""
    lines: List[str] = []
    total_keys = 0
    total_time = 0.0
    max_peak = 0.0

    lines.append("=" * 72)
    lines.append("GFD-ADAPTED BASELINE — PG-Key (internally-described) discovery")
    lines.append(f"min-support = {min_support}")
    lines.append("=" * 72)

    for res in results:
        total_time += res.elapsed_s
        if res.peak_kib is not None:
            max_peak = max(max_peak, res.peak_kib)

        lines.append("")
        lines.append(f"FOR {res.pattern}")
        mem = f", peak {res.peak_kib:.1f} KiB" if res.peak_kib is not None else ""
        lines.append(f"  match: {res.n_matches} | time: {res.elapsed_s*1000:.2f} ms{mem}")

        if res.status == "no_matches":
            lines.append("  [skip] no match for this sub-pattern")
            continue
        if res.status == "below_support":
            lines.append(f"  [skip] support {res.n_matches} < min-support {min_support}")
            continue
        if res.status == "no_key":
            lines.append("  no key found")
            continue

        for key in res.keys:
            total_keys += 1
            lines.append(f"  IDENTIFIER {_format_key(key)}")

    lines.append("")
    lines.append("-" * 72)
    lines.append(f"Total PG-Keys found    : {total_keys}")
    lines.append(f"Sub-patterns examined  : {len(results)}")
    lines.append(f"total execution time: {total_time:.2f}s")

    if max_peak > 0:
        lines.append(f"max total memory: {max_peak:.1f} KiB")
    lines.append("-" * 72)

    return "\n".join(lines)
