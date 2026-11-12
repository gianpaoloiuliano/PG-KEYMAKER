"""
ged_baseline.py
===============
Scoperta delle chiavi minimali esatte sulla tabella dei match di un pattern,
secondo la fase `mineDependencies` di DisGEDs (Liu et al., Information Systems
125, 2024) specializzata alle chiavi (modalita' GKey, §6.3 del paper).

Una chiave K corrisponde al GED sul pattern raddoppiato
    (Q[x, x'], {x.A = x'.A | A in K} -> x.id = x'.id)
Invece di materializzare le coppie di match (n^2 righe) si usa la condizione
equivalente sui match singoli:

    K e' chiave  <=>  la partizione dei match per i valori di K ha solo classi
                      di un elemento (la partizione "stripped" e' vuota).

Componenti (Procedure 2 / Function 2-3 di DisGEDs):
  - MANDATORY : si scartano a monte le colonne con almeno un valore nullo
                (stessa regola della baseline GFD)
  - SINGLETON : le proprieta' a valore lista sono gia' tuple (confrontabili)
  - partizioni stripped per singolo attributo (classi con >= 2 match)
  - lattice level-wise: il nodo X1 u X2 (Definition 3) ha come partizione il
    prodotto delle partizioni dei genitori, in tempo lineare come in TANE
    (Huhtala et al., 1999)
  - validazione: partizione vuota => chiave, non si espande; un candidato e'
    generato solo se tutti i suoi sottoinsiemi sono non-chiavi, quindi
    l'output e' gia' l'insieme delle chiavi minimali (equivalente di findCover)

Semantica di matching: DisGEDs usa l'omomorfismo, Cypher esclude la ripetizione
di archi ma ammette nodi ripetuti. Sui pattern a cammino gli insiemi di match
coincidono.
"""

from typing import Callable, Dict, FrozenSet, List, Optional, Tuple

import numpy as np
import pandas as pd

# Partizione stripped: classi (array di indici di riga) con almeno 2 elementi
Partition = List[np.ndarray]


# ----------------------------------------------------------------------------
# MANDATORY
# ----------------------------------------------------------------------------

def mandatory_columns(df: pd.DataFrame) -> List[str]:
    """Colonne senza alcun NULL: le uniche candidabili a far parte di una chiave."""
    return [c for c in df.columns if df[c].notna().all()]


# ----------------------------------------------------------------------------
# Partizioni
# ----------------------------------------------------------------------------

def _strip_codes(codes: np.ndarray) -> Partition:
    """Partizione stripped a partire dai codici di classe di ogni riga."""
    order = np.argsort(codes, kind="stable").astype(np.int32)
    bounds = np.flatnonzero(np.diff(codes[order])) + 1
    return [cls for cls in np.split(order, bounds) if len(cls) > 1]


def single_partition(series: pd.Series) -> Partition:
    """Partizione stripped dei match per il valore di un attributo."""
    codes, _ = pd.factorize(series, sort=False)
    return _strip_codes(codes)


def product(p1: Partition, p2: Partition, probe: np.ndarray) -> Partition:
    """
    Prodotto di due partizioni stripped (TANE), lineare nella dimensione delle
    partizioni. `probe` e' un buffer int32 di n_rows elementi a -1, riportato a
    -1 al termine (evita di riallocarlo a ogni prodotto).
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
    Livello i+1 del lattice: unione di due non-chiavi del livello i che
    differiscono per l'ultimo attributo (prefisso comune, nessun attributo
    ripetuto). Il candidato e' ammesso solo se tutti i suoi sottoinsiemi di
    taglia i sono non-chiavi: cosi' nessun sovrainsieme di una chiave viene
    generato.
    """
    ordered = sorted(non_keys)
    nxt: Dict[Tuple[int, ...], Partition] = {}
    for a in range(len(ordered)):
        x1 = ordered[a]
        for b in range(a + 1, len(ordered)):
            x2 = ordered[b]
            if x1[:-1] != x2[:-1]:
                break                       # ordinati: il prefisso non torna piu'
            cand = x1 + (x2[-1],)
            if all(cand[:j] + cand[j + 1:] in non_keys for j in range(len(cand) - 2)):
                nxt[cand] = product(non_keys[x1], non_keys[x2], probe)
    return nxt


def find_minimal_keys(
    df: pd.DataFrame,
    on_level: Optional[Callable[[], None]] = None,
) -> List[FrozenSet[str]]:
    """
    Trova tutte le chiavi minimali (insiemi di colonne 'var.prop') del pattern.
    `on_level` e' invocata a ogni livello del lattice (campionamento memoria).
    Ritorna una lista deterministica di frozenset, ordinata per taglia.
    """
    n_rows = len(df)
    if n_rows == 0:
        return []

    cols = mandatory_columns(df)
    if not cols:
        return []

    probe = np.full(n_rows, -1, dtype=np.int32)
    keys: List[FrozenSet[str]] = []

    # livello 1: attributi singoli
    non_keys: Dict[Tuple[int, ...], Partition] = {}
    for i, c in enumerate(cols):
        part = single_partition(df[c])
        if part:
            non_keys[(i,)] = part
        else:
            keys.append(frozenset([c]))
    if on_level:
        on_level()

    # livelli successivi
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
    # Test rapido su una relazione sintetica.
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
