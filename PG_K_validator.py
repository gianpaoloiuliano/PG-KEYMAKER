import networkx as nx
from itertools import product
import itertools
import neo4j_querying_module as n4j
import metanome_ucc as ucc
import query_filters as qf
import time


def check_keys_all_leaf(G,driver,tempi,ucc_alg):
    start = time.time()
    analyzed = {}
    for elem in G.nodes():
        if G.nodes[elem]["label"] in {"node","edge"}:
            #print("elem: ", G.nodes[elem])
            if len(G.nodes[elem]["sub_key"]) == 0:
                if G.nodes[elem]["label"] == "node":
                    original_label = G.nodes[elem].get("original_label")
                    # la cache distingue foglie con la stessa label ma filtri diversi
                    cache_key = (original_label, qf.filter_signature(G.nodes[elem].get("filters"), G.nodes[elem].get("alias")))
                    if cache_key[1]:
                        qf.dbg(f"cache chiavi esterne {elem}: {'HIT' if cache_key in analyzed else 'MISS'}", level=2)
                    if cache_key not in analyzed:
                        #print("sono senza chiave, vedo esterno",elem)
                        external_entities = n4j.matching_external_candidates_in_graph_v2(elem,driver,ucc_alg)
                        analyzed[cache_key] = elem
                        if len(external_entities) > 0:
                            G.nodes[elem]["external_key"] = True
                            G.nodes[elem]["sub_key"] = external_entities[2]
                            G.nodes[elem].get("info_for_ext_match").append(f"external keys from {external_entities[0]} via {external_entities[1]}")
                            #print("external keys found:", G.nodes[elem]["sub_key"])
                            print("info for ext matching:", G.nodes[elem]["info_for_ext_match"])
                    else:
                        print(f"[DEBUG] else: copia da {analyzed[cache_key]} -> {elem}")
                        src_node = analyzed[cache_key]
                        # copia diretta (nessun alias fix)
                        for key in ("external_key", "sub_key", "info_for_ext_match"):
                            value = G.nodes[src_node].get(key)
                            if isinstance(value, list):
                                G.nodes[elem][key] = list(value)
                            else:
                                G.nodes[elem][key] = value

                    '''
                    print("external entities",external_entities,one_to_one_path)
                    for ent in external_entities:
                        #print("ent",ent)
                        key = ucc.compute_keys(ent[2])
                        print("key di external ent",key)
                        if len(key) > 0:
                            G.nodes[elem]["external_key"] = True
                            # info external match update
                            for k in key:
                                #print("k ESTERNO",k)
                                G.nodes[elem]["sub_key"].append(k)
                                #if len(k) == 1:
                                #    print("k",k[0])
                                #    print("elem",elem)
                                #    alias_target = elem.split(":")
                                #    alias_target_clean = alias_target[0].replace("(", "")
                                #    print("alias_target_clean", alias_target_clean)
                                #    tmp = k[0].split(".")
                                #    #print("tmp di 0", tmp[0])
                                #    #print("tmp di 1", tmp[1])
                            #tupla = (one_to_one_path)
                            #print(tupla)
                            G.nodes[elem].get("info_for_ext_match").append((ent[0],ent[1]))
                            print("info ext match",G.nodes[elem].get("info_for_ext_match"))
                        '''
    tempi.ex_external += round(time.time() - start, 2)
    return G,tempi

def check_keys_all_leaf_notree(G,driver,tempi,ucc_alg):
    start = time.time()
    analyzed = {}
    for elem in G.nodes():
        if G.nodes[elem]["label"] in {"node","edge"}:
            #print("elem: ", G.nodes[elem])
            if len(G.nodes[elem]["sub_key"]) == 0:
                if G.nodes[elem]["label"] == "node":
                    original_label = G.nodes[elem].get("original_label")
                    # la cache distingue foglie con la stessa label ma filtri diversi
                    cache_key = (original_label, qf.filter_signature(G.nodes[elem].get("filters"), G.nodes[elem].get("alias")))
                    if cache_key[1]:
                        qf.dbg(f"cache chiavi esterne {elem}: {'HIT' if cache_key in analyzed else 'MISS'}", level=2)
                    if cache_key not in analyzed:
                        #print("sono senza chiave, vedo esterno",elem)
                        external_entities = n4j.matching_external_candidates_in_graph_v2(elem,driver,ucc_alg)
                        #analyzed[cache_key] = elem
                        if len(external_entities) > 0:
                            G.nodes[elem]["external_key"] = True
                            G.nodes[elem]["sub_key"] = external_entities[2]
                            G.nodes[elem].get("info_for_ext_match").append(f"external keys from {external_entities[0]} via {external_entities[1]}")
                            #print("external keys found:", G.nodes[elem]["sub_key"])
                            print("info for ext matching:", G.nodes[elem]["info_for_ext_match"])
                    else:
                        print(f"[DEBUG] else: copia da {analyzed[cache_key]} -> {elem}")
                        src_node = analyzed[cache_key]
                        # copia diretta (nessun alias fix)
                        for key in ("external_key", "sub_key", "info_for_ext_match"):
                            value = G.nodes[src_node].get(key)
                            if isinstance(value, list):
                                G.nodes[elem][key] = list(value)
                            else:
                                G.nodes[elem][key] = value

                    '''
                    print("external entities",external_entities,one_to_one_path)
                    for ent in external_entities:
                        #print("ent",ent)
                        key = ucc.compute_keys(ent[2])
                        print("key di external ent",key)
                        if len(key) > 0:
                            G.nodes[elem]["external_key"] = True
                            # info external match update
                            for k in key:
                                #print("k ESTERNO",k)
                                G.nodes[elem]["sub_key"].append(k)
                                #if len(k) == 1:
                                #    print("k",k[0])
                                #    print("elem",elem)
                                #    alias_target = elem.split(":")
                                #    alias_target_clean = alias_target[0].replace("(", "")
                                #    print("alias_target_clean", alias_target_clean)
                                #    tmp = k[0].split(".")
                                #    #print("tmp di 0", tmp[0])
                                #    #print("tmp di 1", tmp[1])
                            #tupla = (one_to_one_path)
                            #print(tupla)
                            G.nodes[elem].get("info_for_ext_match").append((ent[0],ent[1]))
                            print("info ext match",G.nodes[elem].get("info_for_ext_match"))
                        '''
    tempi.ex_external += round(time.time() - start, 2)
    return G,tempi


def validate_subkeys(G,pattern_father,subkey_set,info_for_ext_match):
    #print("subkey set",subkey_set)
    #print("g prima", G.nodes[pattern_father].get("sub_key"))
    are_external = G.nodes[pattern_father].get("external_key")
    #print("tripla è " + pattern_father + " ed are_external è: " + str(are_external))
    if len(info_for_ext_match) == 0:
        #print("len di info_for_ext",len(info_for_ext_match))
        matched_data = n4j.matching_pattern_for_validation(pattern_father,are_external,[])
        subkey_set = generate_combinations(subkey_set)

        for key in subkey_set:
            '''
            count = matched_data.groupby(key).size()
            if (count == 1).all():
                is_key = True
                #print("g",G.nodes[pattern_father].get("sub_key"))
                G.nodes[pattern_father].get("sub_key").append(key)
            '''
            if not matched_data.duplicated(subset=key).any():
                G.nodes[pattern_father].get("sub_key").append(key)
            else:
                is_key = False
        print(f"tripla considerata:",G.nodes[pattern_father].get("pattern"))
        #print(f"nuove chiavi per la tripla: ",G.nodes[pattern_father].get("sub_key"))
        #triple_key = ucc.compute_keys(matched_data)
        #print("chiavi valide con ucc",triple_key)
        print("\n")
    else:
        bindings = []
        '''
        for target in G.nodes[pattern_father]["info_for_ext_match"].keys():
            print("target",target)
            alias_target = target.split(":")
            alias_target_clean = alias_target[0].replace("(","")
            print("alias_target_clean", alias_target_clean)
            for elem in G.nodes[pattern_father]["info_for_ext_match"][target]:
                print("stampo elem",elem[0])
                tmp = elem[0].split(".")
                if tmp[0] not in pattern_father:
                    print("tmp di 0", tmp[0])
                    print("tmp di 1", tmp[1])
                    tupla = (alias_target_clean,tmp[0].capitalize(),tmp[1])
                    print(tupla)
        '''
        for tupla in info_for_ext_match:
            if tupla[1] not in pattern_father:
                #print("tupla[1] e pattern_father " + tupla[1] +" " + pattern_father)
                bindings.append(tupla)
        #print("bindings",bindings)
        matched_data = n4j.matching_pattern_for_validation(pattern_father,are_external,bindings)
        #print(matched_data)
        set_keys_tmp = set()
        subkey_set_clean = []

        for item in subkey_set:
            t = tuple(item)
            if t not in set_keys_tmp:
                set_keys_tmp.add(t)
                subkey_set_clean.append(item)  # Manteniamo la lista, non la tupla
        subkey_set = subkey_set_clean
        subkey_set = generate_combinations(subkey_set)
        for key in subkey_set:
            #print("key: ",key)
            #print(G.nodes[pattern_father].get("matching_res").duplicated(subset=key).any())
            '''
            count = matched_data.groupby(key).size()
            if (count == 1).all():
                is_key = True
                #print("key valida",key)
                G.nodes[pattern_father].get("sub_key").append(key)
                #print("g", G.nodes[pattern_father].get("sub_key"))
            '''
            if not matched_data.duplicated(subset=key).any():
                G.nodes[pattern_father].get("sub_key").append(key)
                for tupla in info_for_ext_match:
                    #print("tupla",tupla)
                    tpl_clean = tupla[1] +"."+tupla[2]
                    #tpl_clean = tpl_clean.replace("[","")
                    #tpl_clean = tpl_clean.replace("]","")
                    tpl_clean = tpl_clean.lower()
                    #print("tpl_clean",tpl_clean)
                    if key == tpl_clean:
                        G.nodes[pattern_father].get("info_for_ext_match").append(tupla)
                        #print("info ext nuovo",G.nodes[pattern_father].get("info_for_ext_match"))
                '''
                key_parts = key.split(".")
                if key_parts not in pattern_father:
                    G.nodes[pattern_father]["external_key"] = True
                    G.nodes[pattern_father]["info_for_external_matching"] = True
                '''
            else:
                is_key = False
        print(f"tripla considerata:",G.nodes[pattern_father].get("pattern"))
        #print(f"nuove chiavi per la tripla: ",G.nodes[pattern_father].get("sub_key"))
        #triple_key = ucc.compute_keys(matched_data)
        #print("chiavi valide con ucc",triple_key)
        print("\n")
    return G


# ============================================================
#  CASE 1 — Concatenazione (tutte le foglie hanno chiavi)
# ============================================================
def concat_keys_from_leaves(G, nodo, foglie_desc):
    if not foglie_desc:
        print("   ⚠️ Nessuna foglia discendente → skip concatenazione")
        return
    print("   ✅ Tutte le foglie discendenti hanno chiavi → CONCAT su foglie")

    key_lists = [G.nodes[f]["sub_key"] for f in foglie_desc]
    new_keys = []

    for combo in product(*key_lists):
        flat = []
        for elem in combo:
            flat.extend(elem)
        new_keys.append(tuple(flat))

    G.nodes[nodo]["sub_key"] = new_keys
    #print(f"   ➕ Chiave composta creata per {nodo}: {new_keys}")


# ============================================================
#  CASE 2 — Validazione classica
# ============================================================
def validate_or_propagate(G, nodo, figli, minimal,driver):
    subkeys_list = []
    info_for_ext_match = []

    for figlio in figli:
        if G.nodes[figlio].get("external_key"):
            G.nodes[nodo]["external_key"] = True
            info_for_ext_match += G.nodes[figlio].get("info_for_ext_match", [])
        else:
            subkeys_list += G.nodes[figlio].get("sub_key", [])

    #print(f"   Chiavi raccolte per validazione: {subkeys_list}")

    if minimal:
        print("   🔍 validate_minimal_subkeys()")
        G, matched_data = validate_minimal_subkeys(G, nodo, subkeys_list,driver)
    else:
        print("  🔎 validate_subkeys()")
        validate_subkeys(G, nodo, subkeys_list, info_for_ext_match)

    return subkeys_list,matched_data


# ============================================================
#  CASE 3 — Fallback combinatorio (solo se validazione fallisce)
# ============================================================
from itertools import combinations


# ============================================================
#  MAIN FUNCTION — compute_keys_by_structure
# ============================================================
def compute_keys_by_structure(G, minimal,driver,tempi,ucc_alg):

    foglie = {n for n, d in G.nodes(data=True) if d.get("label") in ("node", "edge")}
    G_inv = G.reverse(copy=True)

    # Livelli bottom-up
    livelli = {}
    for nodo in nx.topological_sort(G_inv):
        if nodo in foglie:
            livelli[nodo] = 0
        else:
            figli = list(G_inv.successors(nodo))
            figli_liv = [f for f in figli if f in livelli]
            livelli[nodo] = 1 + max(livelli[f] for f in figli_liv) if figli_liv else 0

    nodi_ordinati = sorted(livelli, key=lambda n: livelli[n])

    # --------------------------------------------------------
    # LOGICA DI CALCOLO CHIAVI PER OGNI NODO
    # --------------------------------------------------------
    def calcola_chiavi(nodo, driver, tempi,ucc_alg):
        label = G.nodes[nodo].get("label")
        if label in ("node", "edge"):
            return

        figli = list(G.successors(nodo))
        disc = nx.descendants(G, nodo)
        foglie_desc = [d for d in disc if d in foglie]

        #print(f"\n➡️ Node: {nodo}")
        #print(f"   Livello: {livelli[nodo]}")
        #print(f"   Figli: {figli}")
        #print(f"   Foglie discendenti: {foglie_desc}")

        all_foglie_have_keys = all(
            len(G.nodes[f].get("sub_key", [])) > 0
            for f in foglie_desc
        )

        # =========================
        # CASO 1
        # =========================
        if all_foglie_have_keys:
            start = time.time()
            result = concat_keys_from_leaves(G, nodo, foglie_desc)
            tempi.ex_case1 += round(time.time() - start,2)
            return result

        # =========================
        # CASO 2
        # =========================
        start = time.time()
        subkeys_list, matched_data = validate_or_propagate(
            G, nodo, figli, minimal, driver
        )
        tempi.ex_case2 += round(time.time() - start,2)

        # =========================
        # CASO 3 (fallback)
        # =========================
        if G.nodes[nodo].get("empty_scope"):
            print("   ⚠️ Scope vuoto → nessun fallback, nessuna chiave")
        elif len(G.nodes[nodo].get("sub_key", [])) == 0:
            print("   ⚠️ Nessuna chiave trovata → attivo FALLBACK combinatorio")

            start = time.time()
            G.nodes[nodo]["sub_key"] = ucc.compute_keys(matched_data,ucc_alg)
            tempi.ex_case3 += round(time.time() - start,2)

        #print(f"   ✅ Chiavi finali: {G.nodes[nodo].get('sub_key', [])}")

    # Esegui bottom-up
    for nodo in nodi_ordinati:
        calcola_chiavi(nodo,driver,tempi,ucc_alg)

    return G,tempi


def validate_minimal_subkeys(G, pattern_father, subkey_set,driver):
    #print("subkey set", subkey_set)
    print("pattern pather",pattern_father + qf.filters_label(G.nodes[pattern_father].get("filters")))
    are_external = G.nodes[pattern_father].get("external_key")
    filters = G.nodes[pattern_father].get("filters", [])
    #matched_data = n4j.matching_pattern_for_validation(pattern_father, are_external, [],driver)
    #print(matched_data)
    matched_data = None  # Non serve qui
    # ============================================================
    # CASO 1 — Nessuna chiave esterna
    # ============================================================
    #if len(info_for_ext_match) == 0:
    # Elimina duplicati tra le chiavi (come liste, ignorando l'ordine)
    subkey_set_unique = []
    seen = set()
    for item in subkey_set:
        t = tuple(sorted(item))
        if t not in seen:
            seen.add(t)
            subkey_set_unique.append(item)
        # === Primo tentativo: validazione rapida con APOC ===

    try:
        apoc_results, n_rows = n4j.check_key_uniqueness_apoc(pattern_father, subkey_set_unique, driver, filters)
    except Exception as e:
        print(f"[WARN] Errore in check_key_uniqueness_apoc: {e}")
        apoc_results, n_rows = {}, None

    # scope senza match (es. filtro troppo selettivo): nessuna chiave significativa
    if n_rows == 0:
        print(f"[WARN] Nessun match per {pattern_father + qf.filters_label(filters)}: "
              f"scope vuoto, nessuna chiave riportata")
        G.nodes[pattern_father]["empty_scope"] = True
        return G, None

    valid_keys = [list(k) for k, is_unique in apoc_results.items() if is_unique]

    if valid_keys:
        print(f"[FAST] {len(valid_keys)} chiavi uniche trovate via APOC per {pattern_father}")
        chiavi_minimali = valid_keys
        for key in chiavi_minimali:
            G.nodes[pattern_father].get("sub_key").append(key)
    else:
        # 🔸 Fallback solo se tutte le candidate sono fallite
        print(f"[SLOW] Nessuna chiave unica via APOC, eseguo query completa...")
        matched_data = n4j.matching_pattern_for_validation(pattern_father, are_external, [], driver, filters)
        # la query completa può non restituire nulla (filtro troppo selettivo, o APOC fallito):
        # niente righe -> niente UCC da calcolare (HPIValid non produce il file .hg)
        if matched_data is None or matched_data.shape[0] == 0 or matched_data.shape[1] == 0:
            print(f"[WARN] Nessun match per {pattern_father + qf.filters_label(filters)}: "
                  f"scope vuoto, nessuna chiave riportata")
            G.nodes[pattern_father]["empty_scope"] = True
            return G, None
    return G, matched_data


    print(f"tripla considerata:", G.nodes[pattern_father].get("pattern"))
    print(f"nuove chiavi per la tripla: ", G.nodes[pattern_father].get("sub_key"))
    #triple_key = ucc.compute_keys(matched_data)
    #print("chiavi valide con ucc", triple_key)
    print("\n")
    '''
    else:
        print("ho trovato chiavi esterne")
        bindings = []
        for tupla in info_for_ext_match:
            if tupla[1] not in pattern_father:
                bindings.append(tupla)
                break

        print(bindings)
        matched_data = n4j.matching_pattern_for_validation(pattern_father, are_external, bindings,driver)

        # Pulisci subkey_set da duplicati
        seen = set()
        subkey_set_clean = []
        for item in subkey_set:
            t = tuple(sorted(item))
            if t not in seen:
                seen.add(t)
                subkey_set_clean.append(item)

        #subkey_set_combo = generate_unique_combinations(subkey_set_clean)
        #chiavi_minimali = find_minimal_keys(matched_data, subkey_set_combo)
        #chiavi_minimali = find_minimal_keys(matched_data, subkey_set_clean)


        for key in subkey_set_clean:
            G.nodes[pattern_father].get("sub_key").append(key)

            for tupla in info_for_ext_match:
                tpl_clean = tupla[1] + "." + tupla[2]
                tpl_clean = tpl_clean.lower()

                if tpl_clean in key:
                    G.nodes[pattern_father].get("info_for_ext_match").append(tupla)

        print(f"tripla considerata:", G.nodes[pattern_father].get("pattern"))
        #print(f"nuove chiavi per la tripla: ", G.nodes[pattern_father].get("sub_key"))
        #triple_key = ucc.compute_keys(matched_data)
        #print("chiavi valide con ucc", triple_key)
        print("\n")
    return G,matched_data
    '''


def generate_combinations(s):
    # Rimuovo sotto-liste duplicate nell'input
    seen = set()
    s_unici = []
    for sublist in s:
        t = tuple(sublist)
        if t not in seen:
            seen.add(t)
            s_unici.append(sublist)
    risultato = []
    risultato.extend(s_unici)
    # Combino da 2 a n elementi
    for r in range(2, len(s_unici) + 1):
        for combo in itertools.combinations(s_unici, r):
            flat = []
            for sublist in combo:
                for item in sublist:
                    if item not in flat:
                        flat.append(item)
            risultato.append(flat)
    # Rimuovo duplicati ignorando l'ordine
    unique = set()
    risultato_finale = []
    for combo in risultato:
        t = tuple(sorted(combo))
        if t not in unique:
            unique.add(t)
            risultato_finale.append(combo)
    return risultato_finale


