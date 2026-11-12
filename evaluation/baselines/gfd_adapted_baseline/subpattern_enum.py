"""
subpattern_enum.py
==================
Enumerazione dei sotto-pattern contigui di uno scope (analogo del vertical
spawning dell'algoritmo GFD e del Sub-Patterns Tree di PG-Keymaker).

Per uno scope con n nodi su un path lineare:
    n0 -[e0]- n1 -[e1]- n2 ... -[e_{n-2}]- n_{n-1}

un sotto-pattern contiguo e' individuato da una coppia (i, j) con
0 <= i <= j <= n-1, e comprende:
    - i nodi  nodes[i .. j]
    - gli edge edges[i .. j-1] (quelli interni all'intervallo)

Esempio, scope  A-[r]-B-[e]-C  (3 nodi):
    (0,0)=A   (1,1)=B   (2,2)=C
    (0,1)=A-[r]-B        (1,2)=B-[e]-C
    (0,2)=A-[r]-B-[e]-C

cioe' 6 sotto-pattern, esattamente come richiesto.
"""

from dataclasses import dataclass
from typing import List

from parser import Node, Relationship, Scope


@dataclass
class SubPattern:
    """Un sotto-percorso contiguo dello scope."""
    nodes: List[Node]
    relationships: List[Relationship]

    def variables(self) -> List[str]:
        """Tutte le variabili (nodi + relazioni) del sotto-pattern, in ordine."""
        return [n.var for n in self.nodes] + [r.var for r in self.relationships]

    def display(self) -> str:
        """Ricostruisce la rappresentazione Cypher-like del sotto-pattern."""
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
        """Numero di nodi del sotto-pattern."""
        return len(self.nodes)


def enumerate_subpatterns(scope: Scope) -> List[SubPattern]:
    """
    Genera tutti i sotto-pattern contigui dello scope, ordinati per dimensione
    crescente (numero di nodi) e, a parita', per posizione di partenza.
    """
    nodes = scope.nodes
    rels = scope.relationships
    n = len(nodes)

    subs: List[SubPattern] = []
    for length in range(1, n + 1):              # numero di nodi del sotto-pattern
        for i in range(0, n - length + 1):      # indice di partenza
            j = i + length - 1                  # indice di fine (incluso)
            sub_nodes = nodes[i:j + 1]
            sub_rels = rels[i:j]                # edge interni all'intervallo
            subs.append(SubPattern(nodes=sub_nodes, relationships=sub_rels))
    return subs


if __name__ == "__main__":
    from parser import parse_scope

    sc = parse_scope("(a:Application:Process)-[r:ROUTES]-(i:Interface)-[e:EXPOSES]-(p:Port)")
    for sp in enumerate_subpatterns(sc):
        print(f"[{sp.size()} nodi] {sp.display()}  vars={sp.variables()}")