"""
parser.py
=========
Parsing of a scope string in Cypher-like notation into a data structure.

Example input:
    (m:Machine)-[r:ROUTES]-(i:Interface)-[e:EXPOSES]-(p:Port)
    (a:Application:Process)-[r:ROUTES]-(i:Interface)

Produces:
    nodes         = [Node('m', ['Machine']), Node('i', ['Interface']), Node('p', ['Port'])]
    relationships = [Relationship('r','ROUTES','m','i','none'),
                     Relationship('e','EXPOSES','i','p','none')]

Handles relationships:
    - bidirectional / undirected:       -[r:T]-
    - forward-directed:                 -[r:T]->
    - backward-directed:                <-[r:T]-
"""

import re
from dataclasses import dataclass, field
from typing import List


@dataclass
class Node:
    """A pattern node: variable + list of labels (supports multi-label)."""
    var: str
    labels: List[str]

    @property
    def label(self) -> str:
        """Backward compatibility: returns the first label (or an empty string)."""
        return self.labels[0] if self.labels else ""

    def cypher_label_str(self) -> str:
        """Returns the Cypher label string, e.g. ':Application:Process'."""
        return "".join(f":{l}" for l in self.labels)


@dataclass
class Relationship:
    """
    A pattern relationship.

    source / target are the variables of the left / right nodes
    in the textual order of the scope.

    direction:
        'right' -> (source)-[r:T]->(target)
        'left'  -> (source)<-[r:T]-(target)
        'none'  -> (source)-[r:T]-(target)
    """
    var: str
    rel_type: str
    source: str
    target: str
    direction: str


@dataclass
class Scope:
    """Linear path: nodes[i] and nodes[i+1] connected by relationships[i]."""
    nodes: List[Node]
    relationships: List[Relationship]


# A node with one or more labels: ( var : Label1 : Label2 : ... )
# Group 2 captures the whole ":Label1:Label2:..." block, or is empty if there are no labels
_NODE_RE = re.compile(r"\(\s*(\w+)((?:\s*:\s*\w+)*)\s*\)")

# A relationship with its connectors
_REL_RE = re.compile(r"(<-|-)\s*\[\s*(\w+)\s*:\s*(\w+)\s*\]\s*(->|-)")


def _parse_labels(labels_str: str) -> List[str]:
    """
    Extracts the list of labels from a string such as ':Label1:Label2'.
    Returns an empty list if the string is empty.
    """
    return [l for l in re.split(r"\s*:\s*", labels_str.strip()) if l]


def _resolve_direction(left: str, right: str) -> str:
    if left == "<-" and right == "->":
        raise ValueError("Relationship with double arrow (<-...->) is not valid")
    if left == "<-":
        return "left"
    if right == "->":
        return "right"
    return "none"


def parse_scope(scope_str: str) -> Scope:
    """
    Parses a Cypher-like scope string into a Scope object.

    Raises ValueError if the string is not a well-formed path.
    """
    if scope_str is None:
        raise ValueError("Null scope")
    s = scope_str.strip()
    if not s:
        raise ValueError("Empty scope")

    nodes: List[Node] = []
    relationships: List[Relationship] = []

    m = _NODE_RE.match(s)
    if not m:
        raise ValueError(
            f"The scope must start with a node '(var:Label)'. Found: {s!r}"
        )
    nodes.append(Node(var=m.group(1), labels=_parse_labels(m.group(2))))
    pos = m.end()

    while pos < len(s):
        while pos < len(s) and s[pos].isspace():
            pos += 1
        if pos >= len(s):
            break

        rm = _REL_RE.match(s, pos)
        if not rm:
            raise ValueError(
                f"Expected a relationship segment at position {pos}: {s[pos:]!r}"
            )
        left, rvar, rtype, right = rm.group(1), rm.group(2), rm.group(3), rm.group(4)
        pos = rm.end()

        nm = _NODE_RE.match(s, pos)
        if not nm:
            raise ValueError(
                f"Expected a node after relationship '{rvar}' at position {pos}: {s[pos:]!r}"
            )
        src_var = nodes[-1].var
        nodes.append(Node(var=nm.group(1), labels=_parse_labels(nm.group(2))))
        dst_var = nodes[-1].var
        pos = nm.end()

        relationships.append(
            Relationship(
                var=rvar,
                rel_type=rtype,
                source=src_var,
                target=dst_var,
                direction=_resolve_direction(left, right),
            )
        )

    all_vars = [n.var for n in nodes] + [r.var for r in relationships]
    dup = {v for v in all_vars if all_vars.count(v) > 1}
    if dup:
        raise ValueError(f"Duplicate variables in the scope: {sorted(dup)}")

    return Scope(nodes=nodes, relationships=relationships)


if __name__ == "__main__":
    sc = parse_scope("(a:Application:Process)-[r:ROUTES]-(i:Interface)-[e:EXPOSES]->(p:Port)")
    print("Nodes:", [(n.var, n.labels) for n in sc.nodes])
    print("Relationships:", [(r.var, r.rel_type, r.source, r.target, r.direction)
                         for r in sc.relationships])