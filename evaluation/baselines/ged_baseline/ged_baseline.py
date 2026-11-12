"""
ged_baseline.py
===============
Discovery of the exact minimal keys on the match table of a pattern,
following the `mineDependencies` phase of DisGEDs (Liu et al., Information
Systems 125, 2024) specialized to keys (GKey mode, §6.3 of the paper).

A key K corresponds to the GED on the doubled pattern
    (Q[x, x'], {x.A = x'.A | A in K} -> x.id = x'.id)
Instead of materializing the pairs of matches (n^2 rows), the equivalent
condition on single matches is used:

    K is a key  <=>  the partition of the matches by the values of K has only
                     singleton classes (the "stripped" partition is empty).

Components (Procedure 2 / Function 2-3 of DisGEDs):
  - MANDATORY : columns with at least one null value are discarded upfront
                (same rule as the GFD baseline)
  - SINGLETON : list-valued properties are already tuples (comparable)
  - stripped partitions for each single attribute (classes with >= 2 matches)
  - level-wise lattice: node X1 u X2 (Definition 3) has as partition the
    product of the parents' partitions, in linear time as in TANE
    (Huhtala et al., 1999)
  - validation: empty partition => key, not expanded; a candidate is
    generated only if all its subsets are non-keys, so the output is
    already the set of minimal keys (equivalent of findCover)

Matching semantics: DisGEDs uses homomorphism, Cypher excludes edge repetition
but allows repeated nodes. On path patterns the sets of matches coincide.
"""

from typing import Callable, Dict, FrozenSet, List, Optional, Tuple

import numpy as np
import pandas as pd

# Stripped partition: classes (arrays of row indices) with at least 2 elements
Partition = List[np.ndarray]


# ----------------------------------------------------------------------------
# MANDATORY
# ----------------------------------------------------------------------------

def mandatory_columns(df: pd.DataFrame) -> List[str]:
    """Columns without any NULL: the only ones that can be part of a key."""
    return [c for c in df.columns if df[c].notna().all()]


# ----------------------------------------------------------------------------
# Partitions
# ----------------------------------------------------------------------------

def _strip_codes(codes: np.ndarray) -> Partition:
    """Stripped partition from the class codes of each row."""
    order = np.argsort(codes, kind="stable").astype(np.int32)
    bounds = np.flatnonzero(np.diff(codes[order])) + 1
    return [cls for cls in np.split(order, bounds) if len(cls) > 1]


def single_partition(series: pd.Series) -> Partition:
    """Stripped partition of the matches by the value of an attribute."""
    codes, _ = pd.factorize(series, sort=False)
    return _strip_codes(codes)


def product(p1: Partition, p2: Partition, probe: np.ndarray) -> Partition:
    """
    Product of two stripped partitions (TANE), linear in the size of the
    partitions. `probe` is an int32 buffer of n_rows elements set to -1, reset
    to -1 at the end (avoids reallocating it at every product).
    """
    for i, cls in enumerate(p1):
        probe[cls] = i

    result: Partition = []
    for cls in p2:
        ids = probe[cls]
        mask = ids >= 0
        sel, ids = cls[mask], ids[mask]
        if len(sel) < 2:
            continue
        order = np.argsort(ids, kind="stable")
        sel, ids = sel[order], ids[order]
        bounds = np.flatnonzero(np.diff(ids)) + 1
        result.extend(g for g in np.split(sel, bounds) if len(g) > 1)

    for cls in p1:
        probe[cls] = -1
    return result


# ----------------------------------------------------------------------------
# Lattice
# ----------------------------------------------------------------------------

def _next_level(
    non_keys: Dict[Tuple[int, ...], Partition],
    probe: np.ndarray,
) -> Dict[Tuple[int, ...], Partition]:
    """
    Level i+1 of the lattice: union of two non-keys of level i that differ
    in the last attribute (common prefix, no repeated attribute). The
    candidate is admitted only if all its subsets of size i are non-keys:
    this way no superset of a key is generated.
    """
    ordered = sorted(non_keys)
    nxt: Dict[Tuple[int, ...], Partition] = {}
    for a in range(len(ordered)):
        x1 = ordered[a]
        for b in range(a + 1, len(ordered)):
            x2 = ordered[b]
            if x1[:-1] != x2[:-1]:
                break                       # sorted: the prefix never comes back
            cand = x1 + (x2[-1],)
            if all(cand[:j] + cand[j + 1:] in non_keys for j in range(len(cand) - 2)):
                nxt[cand] = product(non_keys[x1], non_keys[x2], probe)
    return nxt


def find_minimal_keys(
    df: pd.DataFrame,
    on_level: Optional[Callable[[], None]] = None,
) -> List[FrozenSet[str]]:
    """
    Finds all minimal keys (sets of 'var.prop' columns) of the pattern.
    `on_level` is invoked at every level of the lattice (memory sampling).
    Returns a deterministic list of frozensets, sorted by size.
    """
    n_rows = len(df)
    if n_rows == 0:
        return []

    cols = mandatory_columns(df)
    if not cols:
        return []

    probe = np.full(n_rows, -1, dtype=np.int32)
    keys: List[FrozenSet[str]] = []

    # level 1: single attributes
    non_keys: Dict[Tuple[int, ...], Partition] = {}
    for i, c in enumerate(cols):
        part = single_partition(df[c])
        if part:
            non_keys[(i,)] = part
        else:
            keys.append(frozenset([c]))
    if on_level:
        on_level()

    # subsequent levels
    while non_keys:
        level = _next_level(non_keys, probe)
        non_keys = {}
        for x, part in level.items():
            if part:
                non_keys[x] = part
            else:
                keys.append(frozenset(cols[i] for i in x))
        if on_level:
            on_level()

    return sorted(keys, key=lambda k: (len(k), sorted(k)))


if __name__ == "__main__":
    # Quick test on a synthetic relation.
    data = pd.DataFrame(
        {
            "n.id":    [1, 2, 3, 4],
            "n.name":  ["a", "a", "b", "b"],
            "n.code":  ["x", "y", "x", "y"],
            "n.opt":   [None, 1, 2, 3],
        }
    )
    for key in find_minimal_keys(data):
        print("IDENTIFIER:", sorted(key))
