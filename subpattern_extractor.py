import networkx as nx
import matplotlib.pyplot as plt


def extract_all_valid_subpatterns(pattern):
    parts = pattern.split('-')
    subpatterns = set()

    for start in range(0, len(parts) - 2, 2):
        for end in range(start + 2, len(parts), 2):
            sub = parts[start:end + 1]
            if len(sub) % 2 == 1 and len(sub) >= 5:
                subpatterns.add('-'.join(sub))
            elif len(sub) == 3:
                subpatterns.add('-'.join(sub))

    return list(subpatterns)

def build_pattern_graph(pattern):
    G = nx.DiGraph()
    visited = set()

    def recurse(current_pattern):
        if current_pattern in visited:
            return
        visited.add(current_pattern)

        parts = current_pattern.split('-')
        n_elements = len(parts)

        if n_elements == 3:
            node_label = "tripla"
        elif n_elements >= 5 and n_elements % 2 == 1:
            node_label = "subpattern"
        else:
            return

        # Aggiungiamo il nodo (anche root ora)
        if current_pattern not in G:
            G.add_node(current_pattern,
                       label=node_label,
                       pattern=current_pattern,
                       sub_key=[],
                       external_key=False,
                       info_for_ext_match=[],
                       visited=False)

        if node_label == "tripla":
            nodo1, edge, nodo2 = parts
            for element, el_type in zip([nodo1, edge, nodo2], ['node', 'edge', 'node']):
                if element not in G:
                    orig_lbl = element.split(":")
                    orig_lbl_clean = orig_lbl[1].replace(")","")
                    orig_lbl_clean = orig_lbl_clean.replace("]", "")
                    alias = orig_lbl[0].replace("(","")
                    alias = alias.replace("[", "")
                    G.add_node(element,
                               label=el_type,
                               pattern=element,
                               original_label=orig_lbl_clean,
                               alias = alias,
                               external_key=False,
                               info_for_ext_match=[],
                               sub_key=[],
                               visited=False)
                G.add_edge(current_pattern, element)

        # Ricorsione sui figli
        subpatterns = extract_all_valid_subpatterns(current_pattern)
        for sub in subpatterns:
            if sub == current_pattern:
                continue
            recurse(sub)
            G.add_edge(current_pattern, sub)

    # Aggiungiamo esplicitamente il nodo root (pattern iniziale)
    root_parts = pattern.split('-')
    if len(root_parts) == 3:
        root_label = "tripla"
    elif len(root_parts) >= 5 and len(root_parts) % 2 == 1:
        root_label = "original_pattern"
    else:
        return G  # Se il pattern non è valido, restituiamo grafo vuoto

    G.add_node(pattern,
               label=root_label,
               pattern=pattern,
               sub_key=[],
               external_key = False,
               info_for_ext_match=[],
               visited=False)

    # Avviamo la ricorsione sui figli del pattern root
    for sub in extract_all_valid_subpatterns(pattern):
        if sub != pattern:
            recurse(sub)
            G.add_edge(pattern, sub)

    #draw_graph(G)
    return G


def draw_graph(G):
    plt.figure(figsize=(18, 12))
    pos = nx.spring_layout(G, seed=42)

    # Etichetta completa
    labels = {
        node: f"{G.nodes[node]['label']}\n{G.nodes[node]['pattern']}"
        for node in G.nodes
    }

    # Colori per tipo di nodo
    color_map = {
        'tripla': 'skyblue',
        'subpattern': 'lightgreen',
        'node': 'orange',
        'edge': 'lightcoral'
    }
    node_colors = [color_map.get(G.nodes[n]['label'], 'gray') for n in G.nodes]

    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=1500, edgecolors='k')
    nx.draw_networkx_edges(G, pos, arrows=True, arrowstyle='-|>', width=1.2)
    nx.draw_networkx_labels(G, pos, labels=labels, font_size=9, font_family='monospace')

    plt.title("Grafo dei Subpattern, Triple e Componenti (Nodi + Archi)", fontsize=14)
    plt.axis('off')
    plt.tight_layout()
    plt.show()



'''
def extract_longer_patterns_with_triples(pattern):
    parts = pattern.split('-')
    original = '-'.join(parts)
    result = {}

    for start in range(0, len(parts) - 4, 2):  # almeno 5 elementi (tripla+)
        for end in range(start + 4, len(parts), 2):
            sub = parts[start:end+1]
            if len(sub) % 2 == 1:  # deve iniziare e finire con nodo
                subpattern = '-'.join(sub)
                if subpattern == original:
                    continue

                # Costruzione del dizionario interno
                triple_dict = {}
                for i in range(0, len(sub) - 2, 2):
                    tripla_parts = sub[i:i+3]
                    if len(tripla_parts) == 3:
                        tripla_str = '-'.join(tripla_parts)
                        nodi, archi = split_triple(tripla_str)
                        triple_dict[tripla_str] = {
                            "nodi": nodi,
                            "archi": archi
                        }

                result[subpattern] = triple_dict

    return {"longer": result}


def split_subpatterns(pattern):

    parts = pattern.split('-')
    original = '-'.join(parts)

    triples = {}
    longer_patterns = {}

    for start in range(0, len(parts) - 2, 2):
        for end in range(start + 2, len(parts), 2):
            sub = parts[start:end+1]
            if len(sub) % 2 == 1:  # deve iniziare e finire con nodo
                subpattern = '-'.join(sub)
                if subpattern == original:
                    continue
                if len(sub)>3:
                    parts_triple = subpattern.split('-')
                    nodo1, arco, nodo2 = parts_triple
                    nodi = [nodo1, nodo2]
                    archi = [arco]
                    sub_dict = {"nodes":nodi,"edges":archi}
                    triples.update({subpattern: sub_dict})
                    longer_patterns.append(subpattern)
    dic_subpatterns = {"triple": triples, "longer": longer_patterns}

    return dic_subpatterns


def split_triple(triple):
    parts = triple.split('-')
    if len(parts) != 3:
        raise ValueError("La tripla deve essere nella forma nodo-arco-nodo")
    nodo1, arco, nodo2 = parts
    return [nodo1, nodo2], [arco]
'''
