"""
parser.py
=========
Parsing di una stringa scope in notazione Cypher-like verso una struttura dati.

Esempio di input:
    (m:Machine)-[r:ROUTES]-(i:Interface)-[e:EXPOSES]-(p:Port)
    (a:Application:Process)-[r:ROUTES]-(i:Interface)

Produce:
    nodes         = [Node('m', ['Machine']), Node('i', ['Interface']), Node('p', ['Port'])]
    relationships = [Relationship('r','ROUTES','m','i','none'),
                     Relationship('e','EXPOSES','i','p','none')]

Gestisce relazioni:
    - bidirezionali / non direzionate:  -[r:T]-
    - direzionate in avanti:            -[r:T]->
    - direzionate all'indietro:         <-[r:T]-
"""

import re
from dataclasses import dataclass, field
from typing import List


@dataclass
class Node:
    """Un nodo del pattern: variabile + lista di label (supporta multi-label)."""
    var: str
    labels: List[str]

    @property
    def label(self) -> str:
        """Retrocompatibilità: restituisce la prima label (o stringa vuota)."""
        return self.labels[0] if self.labels else ""

    def cypher_label_str(self) -> str:
        """Restituisce la stringa Cypher delle label, es. ':Application:Process'."""
        return "".join(f":{l}" for l in self.labels)


@dataclass
class Relationship:
    """
    Una relazione del pattern.

    source / target sono le variabili dei nodi a sinistra / destra
    nell'ordine testuale dello scope.

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
    """Path lineare: nodes[i] e nodes[i+1] collegati da relationships[i]."""
    nodes: List[Node]
    relationships: List[Relationship]


# Un nodo con una o più label: ( var : Label1 : Label2 : ... )
# Il gruppo 2 cattura tutto il blocco ":Label1:Label2:..." oppure vuoto se no label
_NODE_RE = re.compile(r"\(\s*(\w+)((?:\s*:\s*\w+)*)\s*\)")

# Una relazione con i suoi connettori
_REL_RE = re.compile(r"(<-|-)\s*\[\s*(\w+)\s*:\s*(\w+)\s*\]\s*(->|-)")


def _parse_labels(labels_str: str) -> List[str]:
    """
    Estrae la lista di label da una stringa del tipo ':Label1:Label2'.
    Restituisce lista vuota se la stringa è vuota.
    """
    return [l for l in re.split(r"\s*:\s*", labels_str.strip()) if l]


def _resolve_direction(left: str, right: str) -> str:
    if left == "<-" and right == "->":
        raise ValueError("Relazione con doppia freccia (<-...->) non valida")
    if left == "<-":
        return "left"
    if right == "->":
        return "right"
    return "none"


def parse_scope(scope_str: str) -> Scope:
    """
    Parsa una stringa scope Cypher-like in un oggetto Scope.

    Solleva ValueError se la stringa non è un path ben formato.
    """
    if scope_str is None:
        raise ValueError("Scope nullo")
    s = scope_str.strip()
    if not s:
        raise ValueError("Scope vuoto")

    nodes: List[Node] = []
    relationships: List[Relationship] = []

    m = _NODE_RE.match(s)
    if not m:
        raise ValueError(
            f"Lo scope deve iniziare con un nodo '(var:Label)'. Trovato: {s!r}"
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
                f"Atteso un segmento di relazione alla posizione {pos}: {s[pos:]!r}"
            )
        left, rvar, rtype, right = rm.group(1), rm.group(2), rm.group(3), rm.group(4)
        pos = rm.end()

        nm = _NODE_RE.match(s, pos)
        if not nm:
            raise ValueError(
                f"Atteso un nodo dopo la relazione '{rvar}' alla posizione {pos}: {s[pos:]!r}"
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
        raise ValueError(f"Variabili duplicate nello scope: {sorted(dup)}")

    return Scope(nodes=nodes, relationships=relationships)


if __name__ == "__main__":
    sc = parse_scope("(a:Application:Process)-[r:ROUTES]-(i:Interface)-[e:EXPOSES]->(p:Port)")
    print("Nodi:", [(n.var, n.labels) for n in sc.nodes])
    print("Relazioni:", [(r.var, r.rel_type, r.source, r.target, r.direction)
                         for r in sc.relationships])