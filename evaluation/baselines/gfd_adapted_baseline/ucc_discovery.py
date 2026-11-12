"""
ucc_discovery.py
================
Discovery of the minimal IDENTIFIERs (Unique Column Combinations) on the match
table of a pattern. This is the "horizontal spawning" of the adapted baseline.

A subset of columns S is an IDENTIFIER (internal PG-Key) if:
  - MANDATORY : no NULL value on any column of S, for any match
  - EXCLUSIVE : the projection of the matches on S has no duplicates (uniqueness)
  - SINGLETON : each target has exactly one value for each attribute of S
                (always true for simple / scalar attributes)

Key observation on the MANDATORY pruning
----------------------------------------
MANDATORY is "downward-closed" with respect to column inclusion: if a
column c contains even a single NULL, then ANY set S containing c violates
MANDATORY (S is a superset of {c} and retains that NULL). Hence no IDENTIFIER
can contain c.

Consequently, the per-subset MANDATORY check described in the baseline is
equivalent to: dropping upfront every column with at least one NULL, and then
searching for the minimal UCCs (EXCLUSIVE only) on the remaining columns. This
form is more efficient and is the one implemented here.

EXCLUSIVE pruning (lattice)
---------------------------
EXCLUSIVE is "upward-closed": if S is unique, every superset of S is unique but
NOT minimal  -> anti-monotonicity: once an IDENTIFIER is found, its supersets
are not explored. Conversely, if S is not unique, none of its subsets is
unique (complementary pruning). Both are achieved by Apriori-style candidate
generation (BFS on the lattice, increasing cardinality):
a candidate of size k+1 is considered only if ALL its subsets of size k are
"non-keys" (i.e. none of them was already an IDENTIFIER).
"""

from itertools import combinations
from typing import List, FrozenSet

import pandas as pd


def _is_exclusive(df: pd.DataFrame, cols: List[str], n_rows: int) -> bool:
    """True if the projection of df on `cols` has no duplicate rows."""
    if not cols:
        # The empty set distinguishes the matches only if there is at most one.
        return n_rows <= 1
    # Number of distinct tuples == number of matches  => uniqueness.
    return df.drop_duplicates(subset=cols).shape[0] == n_rows


def _mandatory_columns(df: pd.DataFrame) -> List[str]:
    """Columns without any NULL: the only ones that can be part of a key."""
    return [c for c in df.columns if df[c].notna().all()]


def _generate_next_level(
    non_keys: List[FrozenSet[str]], k: int
) -> List[FrozenSet[str]]:
    """
    Generates the candidates of size k+1 from the non-keys of size k,
    Apriori-style: a candidate is valid only if ALL its subsets of size k
    are non-keys (present in `non_keys`).

    This simultaneously achieves:
      - the minimality pruning (a candidate containing an already found
        IDENTIFIER has a subset missing from non_keys -> discarded);
      - the complementary pruning on non-keys.
    """
    non_key_set = set(non_keys)
    candidates = set()
    m = len(non_keys)
    for a in range(m):
        for b in range(a + 1, m):
            union = non_keys[a] | non_keys[b]
            if len(union) == k + 1:
                candidates.add(union)

    valid: List[FrozenSet[str]] = []
    for cand in candidates:
        if all(frozenset(sub) in non_key_set for sub in combinations(sorted(cand), k)):
            valid.append(cand)
    return valid


def find_minimal_identifiers(df: pd.DataFrame) -> List[FrozenSet[str]]:
    """
    Finds all minimal subsets of columns that are IDENTIFIERs.

    Strategy: BFS on the column lattice (increasing cardinality) with
    Apriori-style anti-monotonic pruning. MANDATORY is handled upfront
    by dropping the columns with NULLs.

    Returns a list of frozensets (each one is a minimal internal PG-Key).
    """
    n_rows = len(df)
    results: List[FrozenSet[str]] = []
    if n_rows == 0:
        return results

    candidate_cols = _mandatory_columns(df)
    if not candidate_cols:
        return results

    keys: List[FrozenSet[str]] = []
    level: List[FrozenSet[str]] = [frozenset([c]) for c in candidate_cols]
    k = 1

    while level:
        non_keys: List[FrozenSet[str]] = []
        for cand in level:
            cols = list(cand)
            if _is_exclusive(df, cols, n_rows):
                keys.append(cand)           # minimal IDENTIFIER (MANDATORY guaranteed)
            else:
                non_keys.append(cand)       # non-key: it can be extended later
        level = _generate_next_level(non_keys, k)
        k += 1

    return keys


if __name__ == "__main__":
    # Quick test on a synthetic relation.
    data = pd.DataFrame(
        {
            "n.id":    [1, 2, 3, 4],       # key (unique, no null)
            "n.name":  ["a", "a", "b", "b"],   # not unique
            "n.code":  ["x", "y", "x", "y"],   # not unique
            "n.opt":   [None, 1, 2, 3],        # has NULL -> never in a key
        }
    )
    # (name, code) together are unique -> key; id alone -> key.
    for key in find_minimal_identifiers(data):
        print("IDENTIFIER:", sorted(key))
