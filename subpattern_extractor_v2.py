import re
import networkx as nx
import matplotlib.pyplot as plt


# ---------------------------------------------------------------------------
# PATTERN TOKENIZER
# ---------------------------------------------------------------------------
# A Cypher-like pattern is an alternating sequence:
#   NODE  CONNECTOR  NODE  CONNECTOR  NODE  ...
#
# where NODE  = "(alias:Label)"
#       CONNECTOR = one of:
#           -[r:R]-    undirected
#           -[r:R]->   right-directed
#           <-[r:R]-   left-directed
#
# The regex captures separately:
#   group 1 → "node"      e.g.  "(a:Account)"
#   group 2 → "connector" e.g.  "-[wd:Withdraw]->"  or "<-[t:Tran]-"
#
# NOTE: tokens are returned in order, alternating (node, conn, node, ...)

_TOKEN_RE = re.compile(
    r'(\([^)]*\))'           # node  →  (alias:Label)  (possibly with multiple labels)
    r'|'
    r'(<-\[[^\]]*\]-)'       # ←  left-directed connector
    r'|'
    r'(-\[[^\]]*\]->)'       # →  right-directed connector
    r'|'
    r'(-\[[^\]]*\]-)'        # -   undirected connector
)


def tokenize_pattern(pattern: str) -> list[str]:
    """
    Splits a pattern into its atomic parts:
      - tokens of type 'node'      → "(alias:Label)"
      - tokens of type 'connector' → "-[r:R]-"  /  "-[r:R]->"  /  "<-[r:R]-"

    Returns the ordered list of tokens (node, conn, node, conn, node ...).
    Raises ValueError if the pattern is not well-formed.
    """
    tokens = [m.group(0) for m in _TOKEN_RE.finditer(pattern)]

    # Minimal validation: must start and end with a node
    if not tokens:
        raise ValueError(f"Pattern not recognized: {pattern!r}")
    if not (tokens[0].startswith('(') and tokens[-1].startswith('(')):
        raise ValueError(
            f"The pattern must start and end with a node '(...)': {pattern!r}"
        )
    # Length: 1, 3, 5, 7 ... (odd)
    if len(tokens) % 2 == 0:
        raise ValueError(
            f"The pattern is not well-formed (odd number of tokens expected): {pattern!r}"
        )
    return tokens


def connector_direction(connector: str) -> str:
    """
    Returns the direction of a connector:
      '>'  → from left node to right node   (e.g. -[r:R]->)
      '<'  → from right node to left node   (e.g. <-[r:R]-)
      ''   → undirected                     (e.g. -[r:R]-)
    """
    if connector.endswith('->'):
        return '>'
    elif connector.startswith('<-'):
        return '<'
    return ''


def connector_raw_token(connector: str) -> str:
    """
    Extracts the '[alias:Label]' part from a connector (stripping the '-' and '<>' arrows).
    Used only to build the edge-node key in the internal graph.
    """
    # Remove prefix "<-" or "-" and suffix "->" or "-"
    inner = re.search(r'\[([^\]]*)\]', connector)
    if inner:
        return f"[{inner.group(1)}]"
    return connector


def tokens_to_pattern(tokens: list[str]) -> str:
    """
    Re-assembles the tokens into a single pattern string
    (identical to the original if the tokens come from tokenize_pattern).
    """
    return ''.join(tokens)


# ---------------------------------------------------------------------------
# SUBPATTERN EXTRACTION
# ---------------------------------------------------------------------------

def extract_all_valid_subpatterns(tokens: list[str]) -> list[list[str]]:
    """
    Given a pattern as a list of tokens, returns all valid sub-patterns
    (at least one triple, i.e. ≥ 3 tokens) different from the pattern itself.

    Each sub-pattern is returned as a list of tokens.
    """
    subpatterns = []
    n = len(tokens)  # odd

    # Nodes are at even indices: 0, 2, 4, ...
    # A valid sub-pattern goes from even index `start` to even index `end`
    for start in range(0, n - 2, 2):          # at least 1 connector → end ≥ start+2
        for end in range(start + 2, n, 2):    # end is the index of a node
            sub = tokens[start:end + 1]       # includes the final node
            if len(sub) >= 3 and sub != tokens:
                subpatterns.append(sub)

    # Deduplication (same token sequence)
    seen = set()
    unique = []
    for sp in subpatterns:
        key = tuple(sp)
        if key not in seen:
            seen.add(key)
            unique.append(sp)

    return unique


# ---------------------------------------------------------------------------
# GRAPH BUILDING
# ---------------------------------------------------------------------------

def _node_id(token: str) -> str:
    """Unique key for a node token."""
    return token


def _edge_id(connector: str) -> str:
    """Unique key for a connector token (arrows stripped)."""
    return connector_raw_token(connector)


def _parse_node_token(token: str) -> tuple[str, str]:
    """
    From "(alias:Label)" extracts (alias, label_clean).
    Supports multiple labels like ":Label1:Label2" → uses the first as the label.
    """
    inner = token.strip('()')
    if ':' in inner:
        alias, rest = inner.split(':', 1)
        # clean label: take everything up to the next ':' (multiple labels)
        label_clean = rest.split(':')[0]
    else:
        alias = inner
        label_clean = inner
    return alias.strip(), label_clean.strip()


def _parse_edge_token(connector: str) -> tuple[str, str, str]:
    """
    From "-[alias:Label]->" extracts (alias, label_clean, direction).
    """
    direction = connector_direction(connector)
    inner = re.search(r'\[([^\]]*)\]', connector)
    if inner:
        content = inner.group(1)
        if ':' in content:
            alias, label_clean = content.split(':', 1)
        else:
            alias = content
            label_clean = content
    else:
        alias = connector
        label_clean = connector
    return alias.strip(), label_clean.strip(), direction


def tokens_alias_index(tokens: list[str]) -> dict[str, int]:
    """Maps alias → token index in the pattern (even = node, odd = edge)."""
    index = {}
    for i, tok in enumerate(tokens):
        alias = _parse_node_token(tok)[0] if i % 2 == 0 else _parse_edge_token(tok)[0]
        index.setdefault(alias, i)
    return index


def _applicable(filters, aliases) -> list:
    """Filters (query_filters.Filter) whose aliases are all contained in `aliases`."""
    return [f for f in filters if f.aliases <= aliases]


def build_pattern_graph(pattern: str, filters: list | None = None) -> nx.DiGraph:
    """
    Builds the sub-pattern graph from a Cypher-like pattern.

    Supports undirected, right-directed, left-directed and mixed patterns, e.g.:
      (a:A)-[r:R]-(b:B)
      (a:A)-[r:R]->(b:B)<-[s:S]-(c:C)
      (a:A)<-[r:R]-(b:B)-[s:S]->(c:C)

    Structure of the resulting graph:
      - Leaf nodes 'node' and 'edge': atomic components of the pattern
      - 'tripla' nodes: sub-patterns of length 3 (triples)
      - 'subpattern' nodes: sub-patterns of length ≥ 5
      - 'original_pattern' node: the complete original pattern

    Each node has the attributes:
      label, pattern, sub_key, external_key, info_for_ext_match, visited
    Leaf nodes 'node' additionally have: original_label, alias
    Leaf nodes 'edge' additionally have: original_label, alias, direction

    `filters` is the list produced by query_filters.parse_filters. Each node
    receives the attributes:
      aliases → aliases contained in the node
      filters → the filters applicable to that node (all their aliases are in the node)
    """
    filters = filters or []
    G = nx.DiGraph()
    visited_patterns: set[tuple] = set()

    try:
        root_tokens = tokenize_pattern(pattern)
    except ValueError as e:
        print(f"[ERROR] {e}")
        return G

    def _add_leaf_nodes(tokens: list[str]):
        """
        Adds to the graph the leaf nodes (node + edge) of a triple or sequence.
        Returns the list of added keys in the order [node, edge, node, ...].
        """
        keys = []
        for i, tok in enumerate(tokens):
            if i % 2 == 0:  # node
                nid = _node_id(tok)
                if nid not in G:
                    alias, label_clean = _parse_node_token(tok)
                    G.add_node(nid,
                               label='node',
                               pattern=tok,
                               original_label=label_clean,
                               alias=alias,
                               aliases={alias},
                               filters=_applicable(filters, {alias}),
                               external_key=False,
                               info_for_ext_match=[],
                               sub_key=[],
                               visited=False)
                keys.append(nid)
            else:  # connector
                eid = _edge_id(tok)
                if eid not in G:
                    alias, label_clean, direction = _parse_edge_token(tok)
                    G.add_node(eid,
                               label='edge',
                               pattern=eid,
                               original_label=label_clean,
                               alias=alias,
                               direction=direction,
                               aliases={alias},
                               filters=_applicable(filters, {alias}),
                               external_key=False,
                               info_for_ext_match=[],
                               sub_key=[],
                               visited=False)
                keys.append(eid)
        return keys

    def recurse(tokens: list[str]):
        """Recursively builds the graph for a sub-pattern."""
        key = tuple(tokens)
        if key in visited_patterns:
            return
        visited_patterns.add(key)

        pattern_str = tokens_to_pattern(tokens)
        n_tokens = len(tokens)

        if n_tokens == 1:
            return  # single node: ignored

        if n_tokens == 3:
            node_label = 'tripla'
        elif n_tokens >= 5 and n_tokens % 2 == 1:
            node_label = 'subpattern'
        else:
            return

        # Add the pattern node if it does not exist
        if pattern_str not in G:
            aliases = set(tokens_alias_index(tokens))
            G.add_node(pattern_str,
                       label=node_label,
                       pattern=pattern_str,
                       aliases=aliases,
                       filters=_applicable(filters, aliases),
                       sub_key=[],
                       external_key=False,
                       info_for_ext_match=[],
                       visited=False)

        if n_tokens == 3:
            # Add leaves and pattern → leaf edges
            leaf_keys = _add_leaf_nodes(tokens)
            for lk in leaf_keys:
                G.add_edge(pattern_str, lk)
        else:
            # Add child sub-patterns
            for sub_tokens in extract_all_valid_subpatterns(tokens):
                if tuple(sub_tokens) == key:
                    continue
                recurse(sub_tokens)
                sub_str = tokens_to_pattern(sub_tokens)
                G.add_edge(pattern_str, sub_str)

    # ----------------------------------------------------------------
    # Root
    # ----------------------------------------------------------------
    n_root = len(root_tokens)
    if n_root == 3:
        root_label = 'tripla'
    elif n_root >= 5 and n_root % 2 == 1:
        root_label = 'original_pattern'
    else:
        print(f"[WARN] Invalid pattern (token length: {n_root}): {pattern!r}")
        return G

    root_str = tokens_to_pattern(root_tokens)
    root_aliases = set(tokens_alias_index(root_tokens))
    G.add_node(root_str,
               label=root_label,
               pattern=root_str,
               aliases=root_aliases,
               filters=_applicable(filters, root_aliases),
               sub_key=[],
               external_key=False,
               info_for_ext_match=[],
               visited=False)

    if root_label == 'original_pattern':
        for sub_tokens in extract_all_valid_subpatterns(root_tokens):
            recurse(sub_tokens)
            sub_str = tokens_to_pattern(sub_tokens)
            G.add_edge(root_str, sub_str)
    else:
        # Pattern is already a triple: add the leaves directly
        leaf_keys = _add_leaf_nodes(root_tokens)
        for lk in leaf_keys:
            G.add_edge(root_str, lk)

    if filters:
        from query_filters import dbg  # local import: query_filters imports this module
        tot = G.number_of_nodes()
        for k, f in enumerate(filters):
            n_nodes = sum(1 for _, d in G.nodes(data=True) if f in d["filters"])
            dbg(f"#{k} applied to {n_nodes}/{tot} tree nodes")
        for n, d in G.nodes(data=True):
            if d["filters"]:
                dbg(f"  {d['label']:<16} {n}  →  {[f.text for f in d['filters']]}", level=2)

    return G


# ---------------------------------------------------------------------------
# QUICK SELF-TEST
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    test_patterns = [
        # undirected (original)
        "(a:Account)-[wd:Withdraw]-(a2:Account)-[tran:Transfer]-(a3:Account)",
        # fully directed →
        "(p:Patient)-[has:HAS_ENCOUNTER]->(en:Encounter)-[hasp:HAS_PROCEDURE]->(pro:Procedure)",
        # mixed ← →
        "(a:A)-[r:R]->(b:B)<-[s:S]-(c:C)",
        # left-directed
        "(a:A)<-[r:R]-(b:B)<-[s:S]-(c:C)",
        # long mixed
        "(os:OS:Process)-[ins:INSTANCE]->(ver:Version)<-[ins2:INSTANCE]-(os2:OS:Process)",
        # long with multiple types
        "(a:A)-[r1:R1]->(b:B)-[r2:R2]-(c:C)<-[r3:R3]-(d:D)",
    ]

    for pat in test_patterns:
        print(f"\n{'='*70}")
        print(f"PATTERN: {pat}")
        try:
            tokens = tokenize_pattern(pat)
            print(f"  Tokens ({len(tokens)}): {tokens}")
            subs = extract_all_valid_subpatterns(tokens)
            print(f"  Extracted sub-patterns ({len(subs)}):")
            for s in subs:
                print(f"    {tokens_to_pattern(s)}")
            G = build_pattern_graph(pat)
            print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
            for n, d in G.nodes(data=True):
                if d['label'] == 'edge':
                    print(f"    [edge] {n!r}  direction={d.get('direction', '')!r}")
        except Exception as e:
            print(f"  ERROR: {e}")
