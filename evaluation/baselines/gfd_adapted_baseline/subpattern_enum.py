"""
subpattern_enum.py
==================
Enumeration of the contiguous sub-patterns of a scope (analogous to the vertical
spawning of the GFD algorithm and to the Sub-Patterns Tree of PG-Keymaker).

For a scope with n nodes on a linear path:
    n0 -[e0]- n1 -[e1]- n2 ... -[e_{n-2}]- n_{n-1}

a contiguous sub-pattern is identified by a pair (i, j) with
0 <= i <= j <= n-1, and comprises:
    - the nodes  nodes[i .. j]
    - the edges  edges[i .. j-1] (those internal to the interval)

Example, scope  A-[r]-B-[e]-C  (3 nodes):
    (0,0)=A   (1,1)=B   (2,2)=C
    (0,1)=A-[r]-B        (1,2)=B-[e]-C
    (0,2)=A-[r]-B-[e]-C

i.e. 6 sub-patterns, exactly as required.
"""

from dataclasses import dataclass
from typing import List

from parser import Node, Relationship, Scope


@dataclass
class SubPattern:
    """A contiguous sub-path of the scope."""
    nodes: List[Node]
    relationships: List[Relationship]

    def variables(self) -> List[str]:
        """All variables (nodes + relationships) of the sub-pattern, in order."""
        return [n.var for n in self.nodes] + [r.var for r in self.relationships]

    def display(self) -> str:
        """Rebuilds the Cypher-like representation of the sub-pattern."""
        n0 = self.nodes[0]
        parts = [f"({n0.var}{n0.cypher_label_str()})"]
        for idx, rel in enumerate(self.relationships):
            if rel.direction == "right":
                parts.append(f"-[{rel.var}:{rel.rel_type}]->")
            elif rel.direction == "left":
                parts.append(f"<-[{rel.var}:{rel.rel_type}]-")
            else:
                parts.append(f"-[{rel.var}:{rel.rel_type}]-")
            nxt = self.nodes[idx + 1]
            parts.append(f"({nxt.var}{nxt.cypher_label_str()})")
        return "".join(parts)

    def size(self) -> int:
        """Number of nodes of the sub-pattern."""
        return len(self.nodes)


def enumerate_subpatterns(scope: Scope) -> List[SubPattern]:
    """
    Generates all contiguous sub-patterns of the scope, sorted by increasing
    size (number of nodes) and, for equal size, by starting position.
    """
    nodes = scope.nodes
    rels = scope.relationships
    n = len(nodes)

    subs: List[SubPattern] = []
    for length in range(1, n + 1):              # number of nodes of the sub-pattern
        for i in range(0, n - length + 1):      # starting index
            j = i + length - 1                  # ending index (inclusive)
            sub_nodes = nodes[i:j + 1]
            sub_rels = rels[i:j]                # edges internal to the interval
            subs.append(SubPattern(nodes=sub_nodes, relationships=sub_rels))
    return subs


if __name__ == "__main__":
    from parser import parse_scope

    sc = parse_scope("(a:Application:Process)-[r:ROUTES]-(i:Interface)-[e:EXPOSES]-(p:Port)")
    for sp in enumerate_subpatterns(sc):
        print(f"[{sp.size()} nodes] {sp.display()}  vars={sp.variables()}")