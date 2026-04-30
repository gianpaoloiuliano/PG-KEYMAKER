import flowserv
import pandas as pd
import re
import metanome_ucc as ucc
import pickle


def save_graph_pickle(graph, filename):
    """ Salva il grafo in un file pickle """
    with open(filename, 'wb') as file:
        pickle.dump(graph, file)

def generate_query_from_pattern(pattern_str):
    pattern_str = pattern_str.strip()

    has_node = '(' in pattern_str
    has_rel = '[' in pattern_str

    # Caso: solo relazione (es. "[r:REL_TYPE]") → completa con nodi
    if has_rel and not has_node:
        pattern_str = f"(a)-{pattern_str}-(b)"
        match_clause = f"MATCH {pattern_str}"

        # Estrai alias relazione
        match = re.search(r'\[([a-zA-Z_][\w]*)\s*:', pattern_str)
        alias = match.group(1) if match else 'r'

        # Aggiungi filtro per evitare duplicati (id(a) < id(b))
        query = match_clause + "\nWHERE id(a) < id(b)\n" + f"RETURN {alias}"
        return query

    # Caso: solo nodo
    if has_node and not has_rel:
        match_clause = f"MATCH {pattern_str}"
        alias_pattern = re.compile(r'\((\w+)\s*:?[\w]*\)')
        aliases = alias_pattern.findall(pattern_str)
        return_clause = f"RETURN {', '.join(aliases)}" if aliases else "RETURN *"
        query = match_clause + " " + return_clause
        return query

    # Caso: cammino completo (nodi + archi)
    match_clause = f"MATCH {pattern_str}"
    alias_pattern = re.compile(r'[\(\[]\s*(\w+)(?:\s*:[^\)\]]+)?\s*[\)\]]')
    alias_order = alias_pattern.findall(pattern_str)
    return_clause = f"RETURN {', '.join(alias_order)}" if alias_order else "RETURN *"
    query = match_clause +" "+ return_clause
    return query

def matching_pattern_in_graph(subpattern_graph,pattern,label,driver,ucc_alg):
    query = generate_query_from_pattern(pattern)
    #print(query)
    with driver.session() as session:
        result = session.run(query)

        # Lista per raccogliere i dati
        records_list = []

        for record in result:
            row_data = {}
            for key, value in record.items():
                if hasattr(value, '_properties'):  # Se è un nodo o una relazione, estrai le proprietà
                    for prop_key, prop_value in value._properties.items():
                        row_data[f"{key}.{prop_key}"] = prop_value
                else:
                    row_data[key] = value  # Se è un valore normale, lo aggiunge direttamente
            records_list.append(row_data)

        # Creazione DataFrame
        df = pd.DataFrame(records_list)
        #print(df)
        #subpattern_graph.nodes[pattern]["matching_res"] = df
        #print(df)
        if not df.empty and (label == "node" or label == "edge"):
            #print(df)
            try:
                keys = ucc.compute_keys(df,ucc_alg)
                #print("keys", keys)
                subpattern_graph.nodes[pattern]["sub_key"] = keys
            except flowserv.error.FlowservError as e:
                #print("eccezione lanciata: non ci sono ucc, no chiavi!")
                subpattern_graph.nodes[pattern]["sub_key"] = []
        else:
            subpattern_graph.nodes[pattern]["sub_key"] = []

        return subpattern_graph
    #print(f"risultato del matching: {df}")
    #print(f"chiavi trovate: ",keys)

def matching_complete_pattern(pattern,driver):
    query = generate_query_from_pattern(pattern)
    # print(query)
    with driver.session() as session:
        result = session.run(query)

        # Lista per raccogliere i dati
        records_list = []

        for record in result:
            row_data = {}
            for key, value in record.items():
                if hasattr(value, '_properties'):  # Se è un nodo o una relazione, estrai le proprietà
                    for prop_key, prop_value in value._properties.items():
                        row_data[f"{key}.{prop_key}"] = prop_value
                else:
                    row_data[key] = value  # Se è un valore normale, lo aggiunge direttamente
            records_list.append(row_data)

        # Creazione DataFrame
        df = pd.DataFrame(records_list)
    return df

#def create_query_for_external_key(target_label: str, max_depth: int = 2) -> str:
    match = re.search(r':(\w+)', target_label)
    if match:
        target_label = match.group(1)
    else:
        # se non trova ':', ritorna la stringa così com'è
        target_label = target_label.strip('()')

    """
    Builds a Cypher query to find source node labels that:
    - reach ALL nodes with the given target label
    - follow paths of length between 1 and `max_depth` (undirected)
    - have a one-to-one relationship with each target
    - include the minimum depth used to reach those targets
    - and return only the relationship type that satisfies the 1-to-1 rule
    """

    return f"""
    MATCH (target:{target_label})
    WITH collect(target) AS targets, count(target) AS totalTargets

    UNWIND targets AS t
    MATCH path = (source)-[*1..{max_depth}]-(t)
    WITH source, labels(source) AS sourceLabels, t, path, length(path) AS depth, totalTargets

    UNWIND sourceLabels AS sourceLabel
    WITH sourceLabel, t, source, path, depth, totalTargets
    ORDER BY depth ASC

    WITH sourceLabel, t, collect({{source: source, path: path, depth: depth}}) AS entries, totalTargets
    WHERE size(entries) = 1

    WITH sourceLabel,
         t,
         entries[0].path AS minPath,
         entries[0].depth AS minDepth,
         totalTargets

    WITH sourceLabel,
         collect(minPath) AS allPaths,
         min(minDepth) AS minDepth,
         count(DISTINCT t) AS matchedTargets,
         totalTargets
    WHERE matchedTargets = totalTargets

    // ↓↓↓ Estraggo solo la label della relazione minima ↓↓↓
    UNWIND allPaths AS p
    WITH sourceLabel, minDepth, type(relationships(p)[0]) AS relType

    RETURN DISTINCT
        sourceLabel,
        minDepth,
        collect(DISTINCT relType) AS relTypes
    """.strip()

def create_query_for_external_key(target_label: str, max_depth: int = 2) -> str:
    import re

    match = re.search(r':(\w+)', target_label)
    if match:
        target_label = match.group(1)
    else:
        target_label = target_label.strip('()')

    return f"""
    // 1️⃣ BFS dai target fino a max_depth
    MATCH (t:{target_label})
    MATCH path = (t)-[*1..{max_depth}]-(n)
    WITH
        t,
        n,
        length(path) AS depth,
        labels(n)[0] AS neighborLabel,
        type(relationships(path)[-1]) AS relType

    // 2️⃣ target → neighbor (funzionalità)
    WITH neighborLabel, relType, depth, t, collect(DISTINCT n) AS ns
    WHERE size(ns) = 1

    UNWIND ns AS n

    // 3️⃣ neighbor → target (iniettività)
    WITH neighborLabel, relType, depth, n, collect(DISTINCT t) AS ts
    WHERE size(ts) = 1

    // 4️⃣ profondità minima valida
    RETURN
        neighborLabel,
        relType,
        min(depth) AS minDepth
    ORDER BY minDepth ASC
    """.strip()


def execute_simple_matching(target,driver):
    query = f"MATCH ({target.lower()}:{target}) return {target.lower()}"
    with driver.session() as session:
        result = session.run(query)
        #print(f"Results for label '{target}':\n")
        # Lista per raccogliere i dati
        records_list = []
        for record in result:
            row_data = {}
            for key, value in record.items():
                if hasattr(value, '_properties'):  # Se è un nodo o una relazione, estrai le proprietà
                    for prop_key, prop_value in value._properties.items():
                        row_data[f"{key}.{prop_key}"] = prop_value
                else:
                    row_data[key] = value  # Se è un valore normale, lo aggiunge direttamente
            records_list.append(row_data)
        # Creazione DataFrame
        df = pd.DataFrame(records_list)
    return df

def count_pattern_occurrences(pattern, driver):
    """
    Esegue un MATCH su Neo4j usando il pattern fornito
    e ritorna il numero di occorrenze trovate.
    """
    query = f"MATCH {pattern} RETURN count(*) AS occurrences"
    #print(query)
    with driver.session() as session:
        result = session.run(query)
        record = result.single()
        if record:
            return record["occurrences"]
        return 0

#def matching_external_candidates_in_graph(target_label,driver):
    query = create_query_for_external_key(target_label,max_depth=1)
    #print("external query",query)
    ext_entities = []
    with driver.session() as session:
        result = session.run(query)
        #print(f"Results for label '{target_label}':\n")
        for record in result:
            label = record["neighborLabel"]
            depth = record["minDepth"]
            allpaths = record["relType"]
            print(f"Label: {label}, Min Depth: {depth}, relTypes: {allpaths}")
            df = execute_simple_matching(label,driver)
            tuple = (label,depth,df)
            ext_entities.append(tuple)
    return ext_entities, allpaths


def matching_external_candidates_in_graph_v2(target_label,driver,ucc_alg):
    query = create_query_for_external_key(target_label,max_depth=1)
    external_entities = []
    #print("external query",query)
    ext_entities = []
    with driver.session() as session:
        result = session.run(query)
        #print(f"Results for label '{target_label}':\n")
        for record in result:
            label = record["neighborLabel"]
            depth = record["minDepth"]
            allpaths = record["relType"]
            print(f"Label: {label}, Min Depth: {depth}, relTypes: {allpaths}")
            df = execute_specific_path_matching(target_label, label, allpaths, driver)
            if df.shape[1] == 1:
                col = df.columns[0]
                if df[col].is_unique:
                    # La colonna è una chiave
                    key = []
                    key.append([col])
            else:
                key = ucc.compute_keys(df,ucc_alg)
            if len(key) > 0:
                print("keys esterne: ",key)
                ext_entities.append((label, allpaths, key))
                return ext_entities
            else:
                print("nessuna chiave trovata, continua")
    return ext_entities


def execute_specific_path_matching(source_label: str, neighbor_label: str, rel_type: str,driver):
    """
    Esegue il matching del path minimo tra source e neighbor usando un arco specifico.

    Args:
        source_label: label del nodo sorgente (es. 'Encounter')
        neighbor_label: label del nodo vicino (es. 'Patient')
        rel_type: tipo della relazione (es. 'HAS_PATIENT')
        driver: istanza Neo4j driver

    Returns:
        pd.DataFrame con le proprietà dei nodi e della relazione
    """
    path_pattern = f"{source_label}-[:{rel_type}]-({neighbor_label}:{neighbor_label})"

    def run_query(tx, query):
        result = tx.run(query)
        records_list = []
        for record in result:
            row_data = {}
            for key, value in record.items():
                if hasattr(value, "_properties"):
                    for prop_key, prop_value in value._properties.items():
                        row_data[f"{key}.{prop_key}"] = prop_value
                else:
                    row_data[key] = value
            records_list.append(row_data)
        return pd.DataFrame(records_list)

    query = f"MATCH {path_pattern} RETURN {neighbor_label}"
    with driver.session() as session:
        df = session.execute_read(run_query, query)
    #print(df)
    return df


def build_query_with_external_node(pattern, target_node, external_label, external_props):
    ext_var = external_label.lower()
    # Extract all variable names (node or rel) from the pattern
    vars_in_pattern = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b(?=[^:]*[):])', pattern)

    # Build RETURN clause: all pattern vars + selected props
    props = [f"{ext_var}.{p} AS {ext_var}_{p}" for p in external_props]
    return_clause = ', '.join(vars_in_pattern + props)

    # Final query
    query = (
        f"MATCH {pattern},\n"
        f"      ({target_node})--({ext_var}:{external_label})\n"
        f"RETURN {return_clause}"
    )
    return query

def matching_pattern_for_validation(pattern,external_mode,external_specs,driver):
    if not external_mode:
        #print("non ci sono key esterne, faccio match normale")
        query = generate_query_from_pattern(pattern)
        #print(query)
        #print(query)
        with driver.session() as session:
            result = session.run(query)

            # Lista per raccogliere i dati
            records_list = []

            for record in result:
                row_data = {}
                for key, value in record.items():
                    if hasattr(value, '_properties'):  # Se è un nodo o una relazione, estrai le proprietà
                        for prop_key, prop_value in value._properties.items():
                            row_data[f"{key}.{prop_key}"] = prop_value
                    else:
                        row_data[key] = value  # Se è un valore normale, lo aggiunge direttamente
                records_list.append(row_data)

            # Creazione DataFrame
            df = pd.DataFrame(records_list)
    else:
        #print("ci sono key esterne, query di rinforzo")
        query = build_query_with_multiple_targets_and_external_nodes(pattern,external_specs)
        #print(query)
        with driver.session() as session:
            result = session.run(query)

            # Lista per raccogliere i dati
            records_list = []

            for record in result:
                row_data = {}
                for key, value in record.items():
                    if hasattr(value, '_properties'):  # Se è un nodo o una relazione, estrai le proprietà
                        for prop_key, prop_value in value._properties.items():
                            row_data[f"{key}.{prop_key}"] = prop_value
                    else:
                        row_data[key] = value  # Se è un valore normale, lo aggiunge direttamente
                records_list.append(row_data)

            # Creazione DataFrame
            df = pd.DataFrame(records_list)

    return df


def build_query_with_multiple_external_nodes(pattern, target_node, external_specs):
    """
    Builds a Cypher query from a pattern and multiple external identifiers.
    Args:
        pattern (str): The base pattern with variables (e.g. '(p)-[:STUDIA]->(l)')
        target_node (str): The variable name of the node identified externally (e.g. 'l')
        external_specs (list): List of tuples: (external_label, [prop1, prop2, ...])
    Returns:
        str: The Cypher query string.
    """
    # Extract all variables from the pattern
    vars_in_pattern = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b(?=[^:]*[):])', pattern)

    return_lines = set(vars_in_pattern)
    match_lines = [f"MATCH {pattern}"]

    for label, props in external_specs:
        ext_var = label.lower()
        match_lines.append(f"      ({target_node})--({ext_var}:{label})")
        #print("ext_var:",ext_var)
        return_lines.add(ext_var)
        #return_lines += [f"{ext_var}.{p} AS {ext_var}_{p}" for p in props]

    return_clause = ', '.join(sorted(return_lines))
    query = "\n".join(match_lines) + f"\nRETURN {return_clause}"
    return query

def build_query_with_multiple_targets_and_external_nodes(pattern, external_bindings):
    """
    pattern: '(en:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)'
    external_bindings: es. [('en', 'Patient', ['id','SSN', ...])]
    """

    # Trova tutte le variabili del pattern padre
    pattern_vars = re.findall(r'[\(\[](\w+)[\:\]\)]', pattern)

    match_lines = [f"MATCH {pattern}"]
    return_items = pattern_vars.copy()

    for target_var, external_label, props in external_bindings:

        ext_var = external_label.lower()  # es: 'Patient' -> 'patient'

        # Aggiunge:
        # MATCH (en)--(patient:Patient)
        match_lines.append(f"MATCH ({target_var})--({ext_var}:{external_label})")

        # Aggiunge il nodo esterno tra i risultati
        # solo se non esiste già
        if ext_var not in return_items:
            return_items.append(ext_var)

    query = "\n".join(match_lines) + "\nRETURN " + ", ".join(return_items)
    return query


def check_key_uniqueness_apoc(pattern_father, key_sets, driver):
    """
    Usa APOC per testare in batch quali combinazioni di attributi sono chiavi uniche.
    pattern_father: es. '(p:Persona)-[:STUDIA]->(l:Libro)'
    key_sets: [['p.nome'], ['p.nome','p.cognome'], ...]
    """
    query = """
    UNWIND $key_sets AS key_attrs
    // Crea le versioni aliasate (p.nome -> p_nome)
    WITH key_attrs, 
         [x IN key_attrs | x + " AS " + replace(x, ".", "_")] AS aliased,
         [x IN key_attrs | replace(x, ".", "_")] AS alias_names
    // Costruisci la query Cypher dinamica
    WITH key_attrs, 
         "MATCH " + $pattern + 
         " WITH " + apoc.text.join(aliased, ", ") +
         " RETURN count(*) = count(DISTINCT " + 
         CASE 
             WHEN size(alias_names) > 1 
             THEN "[" + apoc.text.join(alias_names, ", ") + "]" 
             ELSE head(alias_names) 
         END +
         ") AS is_unique" AS q
    CALL apoc.cypher.run(q, {}) YIELD value
    RETURN key_attrs, value.is_unique AS is_unique
    """

    with driver.session() as session:
        res = session.run(query, {"key_sets": key_sets, "pattern": pattern_father})
        return {tuple(r["key_attrs"]): r["is_unique"] for r in res}


