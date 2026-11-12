"""
gfd_adapter.py
==============
Orchestrazione dell'intera baseline GFD-adattata per la scoperta di PG-Keys
interne (internally-described).

Pipeline:
  1. parse dello scope                       (parser.parse_scope)
  2. enumerazione sotto-pattern contigui     (subpattern_enum.enumerate_subpatterns)
  3. per ogni sotto-pattern:
       a. scoperta proprieta' + estrazione match da Neo4j (neo4j_connector)
       b. UCC discovery con pruning anti-monotonico       (ucc_discovery)
       c. misura di tempo e (opzionale) memoria
  4. output testuale raggruppato per sotto-pattern, nel formato
       FOR <pattern> IDENTIFIER <attributi>
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
    """Rende un IDENTIFIER come lista ordinata di attributi 'var.prop'."""
    return ", ".join(sorted(key))


def run_discovery(
    connector: Neo4jConnector,
    scope_str: str,
    min_support: int = 1,
    track_memory: bool = True,
    filters_str: str = "",
) -> List[SubPatternResult]:
    """
    Esegue la scoperta su tutti i sotto-pattern dello scope e ritorna i risultati.
    `filters_str`: filtri Cypher (anche aggregati con PER), come in PG-KEYMAKER;
    a ogni sotto-pattern si applicano solo quelli i cui alias compaiono in esso.
    """
    scope = parse_scope(scope_str)
    subpatterns: List[SubPattern] = enumerate_subpatterns(scope)
    if filters_str:
        qf.dbg(f"filtri: {filters_str}")
    filters = qf.parse_filters(filters_str, scope_str)

    if track_memory and not tracemalloc.is_tracing():
        tracemalloc.start()

    results: List[SubPatternResult] = []

    for sp in subpatterns:
        pattern_str = sp.display()
        sp_filters = qf.applicable_filters(filters, sp.variables())
        if filters:
            print(f"sotto-pattern {pattern_str}{qf.filters_label(sp_filters)}")

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
    """Costruisce il report testuale completo (anche per salvataggio su file)."""
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
        lines.append(f"  match: {res.n_matches} | tempo: {res.elapsed_s*1000:.2f} ms{mem}")

        if res.status == "no_matches":
            lines.append("  [skip] nessun match per questo sotto-pattern")
            continue
        if res.status == "below_support":
            lines.append(f"  [skip] supporto {res.n_matches} < min-support {min_support}")
            continue
        if res.status == "no_key":
            lines.append("  no key found")
            continue

        for key in res.keys:
            total_keys += 1
            lines.append(f"  IDENTIFIER {_format_key(key)}")

    lines.append("")
    lines.append("-" * 72)
    lines.append(f"Totale PG-Keys trovate : {total_keys}")
    lines.append(f"Sotto-pattern esaminati: {len(results)}")
    lines.append(f"tempo d'esecuzione totale:: {total_time:.2f}s")

    if max_peak > 0:
        lines.append(f"memoria massima totale: {max_peak:.1f} KiB")
    lines.append("-" * 72)

    return "\n".join(lines)
