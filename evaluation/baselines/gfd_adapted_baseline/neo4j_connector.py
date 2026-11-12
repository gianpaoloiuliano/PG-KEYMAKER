"""
neo4j_connector.py
==================
Connessione a Neo4j ed esecuzione delle query Cypher necessarie alla baseline:

  1. scoperta delle proprieta' (attributi) presenti su ciascun label di nodo
     e su ciascun tipo di relazione dello scope;
  2. estrazione della tabella dei match per un dato sotto-pattern, come
     pandas.DataFrame con una colonna per ogni attributo di ogni variabile.

La generazione della query Cypher per un sotto-pattern e' funzione pura
(`build_match_clause`, `build_subpattern_query`) e quindi testabile senza un
database attivo.
"""

import os
import sys
from typing import Dict, List, Tuple

import pandas as pd
from neo4j import GraphDatabase

from subpattern_enum import SubPattern

# query_filters sta nella root del progetto (tre livelli sopra questa cartella);
# append e non insert, così i moduli locali (parser, ...) restano prioritari
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
import query_filters as qf


# ----------------------------------------------------------------------------
# Generazione query (funzioni pure)
# ----------------------------------------------------------------------------

def build_match_clause(subpattern: SubPattern) -> str:
    """Costruisce la parte MATCH (il path) per un sotto-pattern."""
    nodes = subpattern.nodes
    rels = subpattern.relationships
    parts = [f"({nodes[0].var}{nodes[0].cypher_label_str()})"]
    for idx, rel in enumerate(rels):
        if rel.direction == "right":
            parts.append(f"-[{rel.var}:{rel.rel_type}]->")
        elif rel.direction == "left":
            parts.append(f"<-[{rel.var}:{rel.rel_type}]-")
        else:
            parts.append(f"-[{rel.var}:{rel.rel_type}]-")
        nxt = nodes[idx + 1]
        parts.append(f"({nxt.var}{nxt.cypher_label_str()})")
    return "".join(parts)


def collect_columns(
    subpattern: SubPattern,
    node_props: Dict[str, List[str]],
    rel_props: Dict[str, List[str]],
) -> List[Tuple[str, str]]:
    """
    Lista ordinata (variabile, proprieta') per tutti gli attributi del
    sotto-pattern: prima i nodi (in ordine), poi le relazioni.

    Per nodi multi-label, la chiave in node_props e' la stringa
    ':Label1:Label2' (come restituita da cypher_label_str).
    """
    columns: List[Tuple[str, str]] = []
    for node in subpattern.nodes:
        key = node.cypher_label_str()
        for prop in node_props.get(key, []):
            columns.append((node.var, prop))
    for rel in subpattern.relationships:
        for prop in rel_props.get(rel.rel_type, []):
            columns.append((rel.var, prop))
    return columns


def build_subpattern_query(
    subpattern: SubPattern, columns: List[Tuple[str, str]], filters=None
) -> str:
    """
    Costruisce la query Cypher completa per estrarre i match di un sotto-pattern.

    Le proprieta' sono quotate con backtick per robustezza (nomi con spazi/caratteri
    speciali). Ogni colonna riceve un alias 'col{i}' che verra' poi rimappato a
    'var.prop' lato pandas.

    `filters` sono i filtri (query_filters.Filter) applicabili al sotto-pattern:
    la parte MATCH ... WHERE ... e' costruita da query_filters.build_match_block.
    """
    match = build_match_clause(subpattern)
    match_block = qf.build_match_block(match, filters)
    if columns:
        returns = ", ".join(
            f"{var}.`{prop}` AS col{i}" for i, (var, prop) in enumerate(columns)
        )
        return f"{match_block}\nRETURN {returns}"
    # Nessun attributo: serve solo il conteggio dei match (per il supporto).
    return f"{match_block}\nRETURN count(*) AS support"


# ----------------------------------------------------------------------------
# Normalizzazione valori
# ----------------------------------------------------------------------------

def _hashable(value):
    """
    Rende i valori hashable/comparabili per drop_duplicates.
    Le proprieta' di Neo4j possono essere liste (array properties): convertite
    in tuple. Eventuali dict (improbabili come property value) -> tuple ordinata.
    """
    if isinstance(value, list):
        return tuple(_hashable(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _hashable(v)) for k, v in value.items()))
    return value


# ----------------------------------------------------------------------------
# Connettore
# ----------------------------------------------------------------------------

class Neo4jConnector:
    def __init__(self, uri: str, user: str, password: str,
                 sample_limit: int = 1000):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._sample_limit = sample_limit
        # cache delle proprieta' scoperte — chiave: cypher_label_str per nodi,
        # rel_type per relazioni
        self._node_prop_cache: Dict[str, List[str]] = {}
        self._rel_prop_cache: Dict[str, List[str]] = {}

    def close(self):
        self._driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # --- scoperta proprieta' -------------------------------------------------

    def node_properties(self, node) -> List[str]:
        """
        Unione delle property key presenti sui nodi che matchano tutte le label
        del nodo (campionato). Accetta un oggetto Node.

        La chiave di cache e' cypher_label_str(), es. ':Application:Process'.
        """
        cache_key = node.cypher_label_str()
        if cache_key in self._node_prop_cache:
            return self._node_prop_cache[cache_key]
        match_clause = f"(n{node.cypher_label_str()})"
        query = (
            f"MATCH {match_clause} WITH keys(n) AS ks LIMIT {self._sample_limit} "
            f"UNWIND ks AS k RETURN collect(DISTINCT k) AS props"
        )
        props = self._single_value(query, "props") or []
        props = sorted(props)
        self._node_prop_cache[cache_key] = props
        return props

    def rel_properties(self, rel_type: str) -> List[str]:
        """Unione delle property key presenti sulle relazioni di un tipo (campionato)."""
        if rel_type in self._rel_prop_cache:
            return self._rel_prop_cache[rel_type]
        query = (
            f"MATCH ()-[r:`{rel_type}`]-() WITH keys(r) AS ks LIMIT {self._sample_limit} "
            f"UNWIND ks AS k RETURN collect(DISTINCT k) AS props"
        )
        props = self._single_value(query, "props") or []
        props = sorted(props)
        self._rel_prop_cache[rel_type] = props
        return props

    def discover_properties(self, subpattern: SubPattern
                            ) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """
        Mappa cypher_label_str->props e rel_type->props per le variabili
        del sotto-pattern.
        """
        node_props = {
            n.cypher_label_str(): self.node_properties(n)
            for n in subpattern.nodes
        }
        rel_props = {
            r.rel_type: self.rel_properties(r.rel_type)
            for r in subpattern.relationships
        }
        return node_props, rel_props

    # --- estrazione match ----------------------------------------------------

    def match_subpattern(self, subpattern: SubPattern, filters=None
                         ) -> Tuple[pd.DataFrame, str, int]:
        """
        Esegue la query del sotto-pattern e restituisce:
          (DataFrame dei match, query Cypher usata, numero di match).

        Le colonne del DataFrame sono rinominate 'var.prop'. Se il sotto-pattern
        non ha alcun attributo, il DataFrame ha 0 colonne ma il numero di righe
        riflette comunque il supporto (per il filtraggio min-support).
        """
        node_props, rel_props = self.discover_properties(subpattern)
        columns = collect_columns(subpattern, node_props, rel_props)
        query = build_subpattern_query(subpattern, columns, filters)

        with self._driver.session() as session:
            result = session.run(query)
            if not columns:
                rec = result.single()
                support = int(rec["support"]) if rec else 0
                empty = pd.DataFrame(index=range(support))  # 0 colonne, `support` righe
                return empty, query, support

            records = []
            for rec in result:
                records.append(
                    [_hashable(rec[f"col{i}"]) for i in range(len(columns))]
                )

        col_names = [f"{var}.{prop}" for (var, prop) in columns]
        df = pd.DataFrame(records, columns=col_names)
        return df, query, len(df)

    # --- util ----------------------------------------------------------------

    def _single_value(self, query: str, key: str):
        with self._driver.session() as session:
            rec = session.run(query).single()
            return rec[key] if rec is not None else None