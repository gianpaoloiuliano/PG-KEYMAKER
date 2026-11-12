"""
ged_adapter.py
==============
Orchestrazione della baseline GED (DisGEDs adattato) per la scoperta di PG-Keys
su uno scope dato.

Pipeline:
  1. parse dello scope                       (parser.parse_scope, baseline GFD)
  2. sotto-pattern come HyUCC/HPIValid: nodi singoli, archi singoli e cammini
     contigui nodo-nodo (subpattern_enum.enumerate_subpatterns + archi)
  3. per ogni sotto-pattern:
       a. match come DataFrame da Neo4j     (neo4j_connector, baseline GFD)
       b. chiavi minimali interne           (ged_baseline.find_minimal_keys)
       c. se nodo singolo senza chiavi interne: chiavi esterne con la stessa
          strategia di HyUCC/HPIValid (vicini, coverage, one-to-one)
       d. misura di tempo e memoria (RSS in MB, come HyUCC/HPIValid)
  4. output testuale nel formato della baseline GFD:
       FOR <pattern> IDENTIFIER <attributi>
"""

import os
import sys
import time
from dataclasses import dataclass, field
from typing import FrozenSet, List, Optional, Tuple

import pandas as pd
import psutil

# le dipendenze (parser, enumerazione, connettore) sono quelle della baseline GFD,
# riusate per import senza modificarle; append cosi' i moduli locali restano prioritari
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
    """Rende un IDENTIFIER come lista ordinata di attributi 'var.prop'."""
    return ", ".join(sorted(key))


# ----------------------------------------------------------------------------
# Sotto-pattern
# ----------------------------------------------------------------------------

def _edge_display(rel: Relationship) -> str:
    return f"[{rel.var}:{rel.rel_type}]"


def _flatten(records, columns: List[str]) -> pd.DataFrame:
    return pd.DataFrame(records, columns=columns)


def match_edge(connector: Neo4jConnector, rel: Relationship, filters=None
               ) -> Tuple[pd.DataFrame, int]:
    """
    Match di un arco singolo: una riga per relazione, colonne 'var.prop'.
    Gli archi non direzionati sono restituiti da Cypher in entrambi gli
    orientamenti: si tiene un solo orientamento con id(s) <= id(t).
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
# Chiavi esterne (stessa strategia di hyucc_baseline.py / hpivalid_baseline.py)
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
    """target_label e' la stringa Cypher delle label, es. ':Application:Process'."""
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
# Scoperta
# ----------------------------------------------------------------------------

def run_discovery(
    connector: Neo4jConnector,
    scope_str: str,
    track_memory: bool = True,
    filters_str: str = "",
) -> List[SubPatternResult]:
    """
    Esegue la scoperta su tutti i sotto-pattern dello scope e ritorna i risultati.
    `filters_str`: filtri Cypher (anche aggregati con PER), come in PG-KEYMAKER;
    a ogni sotto-pattern si applicano solo quelli i cui alias compaiono in esso.
    """
    scope = parse_scope(scope_str)
    paths: List[SubPattern] = enumerate_subpatterns(scope)
    nodes = [sp for sp in paths if sp.size() == 1]
    longer = [sp for sp in paths if sp.size() > 1]
    if filters_str:
        qf.dbg(f"filtri: {filters_str}")
    filters = qf.parse_filters(filters_str, scope_str)

    # stesso ordine di baseline_subpatterns: nodi, archi, cammini
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
            print(f"sotto-pattern {pattern_str}{qf.filters_label(sp_filters)}")

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
            # chiavi esterne: solo nodo singolo senza chiavi interne (come HyUCC)
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
    """Costruisce il report testuale completo (anche per salvataggio su file)."""
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
        lines.append(f"  match: {res.n_matches} | tempo: {res.elapsed_s*1000:.2f} ms{mem}")

        if res.status == "no_matches":
            lines.append("  [skip] nessun match per questo sotto-pattern")
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
    lines.append(f"Totale PG-Keys trovate : {total_keys}")
    lines.append(f"Sotto-pattern esaminati: {len(results)}")
    lines.append(f"tempo d'esecuzione totale:: {total_time:.2f}s")

    if max_peak > 0:
        lines.append(f"memoria massima totale: {max_peak:.2f} MB")
    lines.append("-" * 72)

    return "\n".join(lines)
