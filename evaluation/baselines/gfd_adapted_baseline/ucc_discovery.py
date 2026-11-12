"""
ucc_discovery.py
================
Scoperta degli IDENTIFIER minimali (Unique Column Combinations) sulla tabella
dei match di un pattern. E' l'"horizontal spawning" della baseline adattata.

Un sottoinsieme di colonne S e' un IDENTIFIER (PG-Key interna) se:
  - MANDATORY : nessun valore NULL su nessuna colonna di S, per nessun match
  - EXCLUSIVE : la proiezione dei match su S non ha duplicati (unicita')
  - SINGLETON : ogni target ha esattamente un valore per ogni attributo di S
                (sempre vero per attributi semplici / scalari)

Osservazione chiave sul pruning di MANDATORY
--------------------------------------------
MANDATORY e' "downward-closed" rispetto all'inclusione di colonne: se una
colonna c contiene anche un solo NULL, allora QUALSIASI insieme S che contenga
c viola MANDATORY (S e' un sovrainsieme di {c} e mantiene quel NULL). Dunque
nessun IDENTIFIER puo' contenere c.

Di conseguenza il controllo MANDATORY per-sottoinsieme descritto nella baseline
e' equivalente a: eliminare a monte ogni colonna con almeno un NULL, e poi
cercare gli UCC minimali (solo EXCLUSIVE) sulle colonne rimanenti. Questa
forma e' piu' efficiente ed e' quella implementata qui.

Pruning EXCLUSIVE (reticolo)
----------------------------
EXCLUSIVE e' "upward-closed": se S e' unico, ogni sovrainsieme di S e' unico ma
NON minimale  -> anti-monotonicita': trovato un IDENTIFIER, non si esplorano i
suoi sovrainsiemi. Viceversa, se S non e' unico, ogni suo sottoinsieme non e'
unico (pruning complementare). Entrambi sono realizzati dalla generazione dei
candidati in stile Apriori (BFS sul reticolo, cardinalita' crescente):
un candidato di taglia k+1 e' considerato solo se TUTTI i suoi sottoinsiemi di
taglia k sono "non-chiavi" (cioe' nessuno di essi era gia' un IDENTIFIER).
"""

from itertools import combinations
from typing import List, FrozenSet

import pandas as pd


def _is_exclusive(df: pd.DataFrame, cols: List[str], n_rows: int) -> bool:
    """True se la proiezione di df su `cols` non ha righe duplicate."""
    if not cols:
        # L'insieme vuoto distingue i match solo se ce n'e' al piu' uno.
        return n_rows <= 1
    # Numero di tuple distinte == numero di match  => unicita'.
    return df.drop_duplicates(subset=cols).shape[0] == n_rows


def _mandatory_columns(df: pd.DataFrame) -> List[str]:
    """Colonne senza alcun NULL: le uniche candidabili a far parte di una chiave."""
    return [c for c in df.columns if df[c].notna().all()]


def _generate_next_level(
    non_keys: List[FrozenSet[str]], k: int
) -> List[FrozenSet[str]]:
    """
    Genera i candidati di taglia k+1 a partire dalle non-chiavi di taglia k,
    in stile Apriori: un candidato e' valido solo se TUTTI i suoi sottoinsiemi
    di taglia k sono non-chiavi (presenti in `non_keys`).

    Questo realizza contemporaneamente:
      - il pruning di minimalita' (un candidato che contiene un IDENTIFIER gia'
        trovato ha un sottoinsieme assente da non_keys -> scartato);
      - il pruning complementare sulle non-chiavi.
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
    Trova tutti i sottoinsiemi minimali di colonne che sono IDENTIFIER.

    Strategia: BFS sul reticolo delle colonne (cardinalita' crescente) con
    pruning anti-monotonico in stile Apriori. MANDATORY e' gestito a monte
    eliminando le colonne con NULL.

    Ritorna una lista di frozenset (ognuno e' una PG-Key interna minimale).
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
                keys.append(cand)           # IDENTIFIER minimale (MANDATORY garantito)
            else:
                non_keys.append(cand)       # non-chiave: si potra' estendere
        level = _generate_next_level(non_keys, k)
        k += 1

    return keys


if __name__ == "__main__":
    # Test rapido su una relazione sintetica.
    data = pd.DataFrame(
        {
            "n.id":    [1, 2, 3, 4],       # chiave (unica, no null)
            "n.name":  ["a", "a", "b", "b"],   # non unica
            "n.code":  ["x", "y", "x", "y"],   # non unica
            "n.opt":   [None, 1, 2, 3],        # ha NULL -> mai in una chiave
        }
    )
    # (name, code) insieme e' unico -> chiave; id da solo -> chiave.
    for key in find_minimal_identifiers(data):
        print("IDENTIFIER:", sorted(key))
