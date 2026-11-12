"""
neo4j_connector.py
==================
Connection to Neo4j and execution of the Cypher queries needed by the baseline:

  1. discovery of the properties (attributes) present on each node label
     and on each relationship type of the scope;
  2. extraction of the match table for a given sub-pattern, as a
     pandas.DataFrame with one column for each attribute of each variable.

The generation of the Cypher query for a sub-pattern is a pure function
(`build_match_clause`, `build_subpattern_query`) and is therefore testable
without a running database.
"""

import os
import sys
from typing import Dict, List, Tuple

import pandas as pd
from neo4j import GraphDatabase

from subpattern_enum import SubPattern

# query_filters lives in the project root (three levels above this folder);
# append rather than insert, so that local modules (parser, ...) keep priority
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
import query_filters as qf


# ----------------------------------------------------------------------------
# Query generation (pure functions)
# ----------------------------------------------------------------------------

def build_match_clause(subpattern: SubPattern) -> str:
    """Builds the MATCH part (the path) for a sub-pattern."""
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
    Ordered list of (variable, property) for all attributes of the
    sub-pattern: first the nodes (in order), then the relationships.

    For multi-label nodes, the key in node_props is the string
    ':Label1:Label2' (as returned by cypher_label_str).
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
    Builds the complete Cypher query to extract the matches of a sub-pattern.

    Properties are quoted with backticks for robustness (names with spaces or
    special characters). Each column gets an alias 'col{i}' that is later
    remapped to 'var.prop' on the pandas side.

    `filters` are the filters (query_filters.Filter) applicable to the sub-pattern:
    the MATCH ... WHERE ... part is built by query_filters.build_match_block.
    """
    match = build_match_clause(subpattern)
    match_block = qf.build_match_block(match, filters)
    if columns:
        returns = ", ".join(
            f"{var}.`{prop}` AS col{i}" for i, (var, prop) in enumerate(columns)
        )
        return f"{match_block}\nRETURN {returns}"
    # No attributes: only the match count is needed (for the support).
    return f"{match_block}\nRETURN count(*) AS support"


# ----------------------------------------------------------------------------
# Value normalization
# ----------------------------------------------------------------------------

def _hashable(value):
    """
    Makes values hashable/comparable for drop_duplicates.
    Neo4j properties can be lists (array properties): converted to tuples.
    Any dicts (unlikely as property values) -> sorted tuple.
    """
    if isinstance(value, list):
        return tuple(_hashable(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _hashable(v)) for k, v in value.items()))
    return value


# ----------------------------------------------------------------------------
# Connector
# ----------------------------------------------------------------------------

class Neo4jConnector:
    def __init__(self, uri: str, user: str, password: str,
                 sample_limit: int = 1000):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._sample_limit = sample_limit
        # cache of discovered properties — key: cypher_label_str for nodes,
        # rel_type for relationships
        self._node_prop_cache: Dict[str, List[str]] = {}
        self._rel_prop_cache: Dict[str, List[str]] = {}

    def close(self):
        self._driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # --- property discovery -------------------------------------------------

    def node_properties(self, node) -> List[str]:
        """
        Union of the property keys present on the nodes that match all the
        labels of the node (sampled). Accepts a Node object.

        The cache key is cypher_label_str(), e.g. ':Application:Process'.
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
        """Union of the property keys present on the relationships of a type (sampled)."""
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
        Maps cypher_label_str->props and rel_type->props for the variables
        of the sub-pattern.
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

    # --- match extraction ----------------------------------------------------

    def match_subpattern(self, subpattern: SubPattern, filters=None
                         ) -> Tuple[pd.DataFrame, str, int]:
        """
        Runs the sub-pattern query and returns:
          (DataFrame of the matches, Cypher query used, number of matches).

        The DataFrame columns are renamed 'var.prop'. If the sub-pattern
        has no attributes, the DataFrame has 0 columns but the number of rows
        still reflects the support (for min-support filtering).
        """
        node_props, rel_props = self.discover_properties(subpattern)
        columns = collect_columns(subpattern, node_props, rel_props)
        query = build_subpattern_query(subpattern, columns, filters)

        with self._driver.session() as session:
            result = session.run(query)
            if not columns:
                rec = result.single()
                support = int(rec["support"]) if rec else 0
                empty = pd.DataFrame(index=range(support))  # 0 columns, `support` rows
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