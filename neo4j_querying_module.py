import flowserv
import pandas as pd
import re
import metanome_ucc as ucc
import pickle
import query_filters as qf


def generate_query_from_pattern(pattern_str, filters=None):
    pattern_str = pattern_str.strip()

    has_node = '(' in pattern_str
    has_rel = '[' in pattern_str

    # Case: relationship only (e.g. "[r:REL_TYPE]") → complete with nodes
    if has_rel and not has_node:
        pattern_str = f"(a)-{pattern_str}-(b)"

        # Extract relationship alias
        match = re.search(r'\[([a-zA-Z_][\w]*)\s*:', pattern_str)
        alias = match.group(1) if match else 'r'

        # Add filter to avoid duplicates (id(a) < id(b)), ANDed with any filters
        match_clause = qf.build_match_block(pattern_str, filters, extra_where="id(a) < id(b)")
        query = match_clause + "\n" + f"RETURN {alias}"
        return query

    # Case: node only
    if has_node and not has_rel:
        match_clause = qf.build_match_block(pattern_str, filters)
        alias_pattern = re.compile(r'\((\w+)\s*:?[\w]*\)')
        aliases = alias_pattern.findall(pattern_str)
        return_clause = f"RETURN {', '.join(aliases)}" if aliases else "RETURN *"
        query = match_clause + " " + return_clause
        return query

    # Case: full path (nodes + edges)
    match_clause = qf.build_match_block(pattern_str, filters)
    alias_pattern = re.compile(r'[\(\[]\s*(\w+)(?:\s*:[^\)\]]+)?\s*[\)\]]')
    alias_order = alias_pattern.findall(pattern_str)
    return_clause = f"RETURN {', '.join(alias_order)}" if alias_order else "RETURN *"
    query = match_clause +" "+ return_clause
    return query


def matching_pattern_in_graph(subpattern_graph,pattern,label,driver,ucc_alg):
    filters = subpattern_graph.nodes[pattern].get("filters", [])
    query = generate_query_from_pattern(pattern, filters)
    #print(query)
    with driver.session() as session:
        result = session.run(query)

        # List to collect the data
        records_list = []

        for record in result:
            row_data = {}
            for key, value in record.items():
                if hasattr(value, '_properties'):  # If it is a node or a relationship, extract its properties
                    for prop_key, prop_value in value._properties.items():
                        row_data[f"{key}.{prop_key}"] = prop_value
                else:
                    row_data[key] = value  # If it is a plain value, add it directly
            records_list.append(row_data)

        # DataFrame creation
        df = pd.DataFrame(records_list)
        if filters:
            qf.dbg(f"leaf {pattern}: {len(df)} filtered rows", level=2)
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
                #print("exception raised: there are no uccs, no keys!")
                subpattern_graph.nodes[pattern]["sub_key"] = []
        else:
            subpattern_graph.nodes[pattern]["sub_key"] = []

        return subpattern_graph
    #print(f"matching result: {df}")
    #print(f"keys found: ",keys)


def create_query_for_external_key(target_label: str, max_depth: int = 2) -> str:
    import re

    match = re.search(r':(\w+)', target_label)
    if match:
        target_label = match.group(1)
    else:
        target_label = target_label.strip('()')

    return f"""
    // 1️⃣ BFS from the targets up to max_depth
    MATCH (t:{target_label})
    MATCH path = (t)-[*1..{max_depth}]-(n)
    WITH
        t,
        n,
        length(path) AS depth,
        labels(n)[0] AS neighborLabel,
        type(relationships(path)[-1]) AS relType

    // 2️⃣ target → neighbor (functionality)
    WITH neighborLabel, relType, depth, t, collect(DISTINCT n) AS ns
    WHERE size(ns) = 1

    UNWIND ns AS n

    // 3️⃣ neighbor → target (injectivity)
    WITH neighborLabel, relType, depth, n, collect(DISTINCT t) AS ts
    WHERE size(ts) = 1

    // 4️⃣ minimum valid depth
    RETURN
        neighborLabel,
        relType,
        min(depth) AS minDepth
    ORDER BY minDepth ASC
    """.strip()


def count_pattern_occurrences(pattern, driver, filters=None):
    """
    Runs a MATCH on Neo4j using the given pattern (and any filters)
    and returns the number of occurrences found.
    """
    query = qf.build_match_block(pattern, filters) + " RETURN count(*) AS occurrences"
    #print(query)
    with driver.session() as session:
        result = session.run(query)
        record = result.single()
        if record:
            return record["occurrences"]
        return 0


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
                print("entered the if")
                col = df.columns[0]
                print(col)
                key = []
                if df[col].is_unique:
                    # The column is a key
                    key.append([col])
            else:
                key = ucc.compute_keys(df,ucc_alg)
            #print("keys:",key)
            if len(key) > 0:
                #print("external keys: ",key)
                ext_entities.append((label, allpaths, key))
                return ext_entities
            else:
                print("no key found, continuing")
    return ext_entities


def execute_specific_path_matching(source_label: str, neighbor_label: str, rel_type: str,driver):
    """
    Runs the matching of the minimal path between source and neighbor using a specific edge.

    Args:
        source_label: label of the source node (e.g. 'Encounter')
        neighbor_label: label of the neighbor node (e.g. 'Patient')
        rel_type: relationship type (e.g. 'HAS_PATIENT')
        driver: Neo4j driver instance

    Returns:
        pd.DataFrame with the properties of the nodes and of the relationship
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


def matching_pattern_for_validation(pattern,external_mode,external_specs,driver,filters=None):
    if not external_mode:
        #print("there are no external keys, doing a normal match")
        query = generate_query_from_pattern(pattern, filters)
        #print(query)
        #print(query)
        with driver.session() as session:
            result = session.run(query)

            # List to collect the data
            records_list = []

            for record in result:
                row_data = {}
                for key, value in record.items():
                    if hasattr(value, '_properties'):  # If it is a node or a relationship, extract its properties
                        for prop_key, prop_value in value._properties.items():
                            row_data[f"{key}.{prop_key}"] = prop_value
                    else:
                        row_data[key] = value  # If it is a plain value, add it directly
                records_list.append(row_data)

            # DataFrame creation
            df = pd.DataFrame(records_list)
    else:
        #print("there are external keys, reinforcement query")
        query = build_query_with_multiple_targets_and_external_nodes(pattern,external_specs,filters)
        #print(query)
        with driver.session() as session:
            result = session.run(query)

            # List to collect the data
            records_list = []

            for record in result:
                row_data = {}
                for key, value in record.items():
                    if hasattr(value, '_properties'):  # If it is a node or a relationship, extract its properties
                        for prop_key, prop_value in value._properties.items():
                            row_data[f"{key}.{prop_key}"] = prop_value
                    else:
                        row_data[key] = value  # If it is a plain value, add it directly
                records_list.append(row_data)

            # DataFrame creation
            df = pd.DataFrame(records_list)

    if filters:
        qf.dbg(f"validation {pattern}: {len(df)} filtered rows", level=2)
    return df


def build_query_with_multiple_targets_and_external_nodes(pattern, external_bindings, filters=None):
    """
    pattern: '(en:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)'
    external_bindings: e.g. [('en', 'Patient', ['id','SSN', ...])]
    """

    # Find all the variables of the parent pattern
    pattern_vars = re.findall(r'[\(\[](\w+)[\:\]\)]', pattern)

    match_lines = [qf.build_match_block(pattern, filters)]
    return_items = pattern_vars.copy()

    for target_var, external_label, props in external_bindings:

        ext_var = external_label.lower()  # e.g. 'Patient' -> 'patient'

        # Adds:
        # MATCH (en)--(patient:Patient)
        match_lines.append(f"MATCH ({target_var})--({ext_var}:{external_label})")

        # Add the external node to the results
        # only if it does not already exist
        if ext_var not in return_items:
            return_items.append(ext_var)

    query = "\n".join(match_lines) + "\nRETURN " + ", ".join(return_items)
    return query


def check_key_uniqueness_apoc(pattern_father, key_sets, driver, filters=None):
    """
    Uses APOC to test in batch which attribute combinations are unique keys.
    pattern_father: e.g. '(p:Persona)-[:STUDIA]->(l:Libro)'
    key_sets: [['p.nome'], ['p.nome','p.cognome'], ...]
    Returns (results, n_rows): a key is valid only if the match is non-empty,
    has no duplicates and no row has null values on its attributes (MANDATORY).
    filters: filters applicable to the pattern (query_filters.Filter); the
             MATCH ... WHERE ... part is built in Python and passed as $match_block
    """
    match_block = qf.build_match_block(pattern_father, filters)
    query = """
    UNWIND $key_sets AS key_attrs
    // Create the aliased versions (p.nome -> p_nome)
    WITH key_attrs, 
         [x IN key_attrs | x + " AS " + replace(x, ".", "_")] AS aliased,
         [x IN key_attrs | replace(x, ".", "_")] AS alias_names
    // Build the dynamic Cypher query
    WITH key_attrs, 
         $match_block +
         " WITH " + apoc.text.join(aliased, ", ") +
         " RETURN count(*) AS n, count(*) > 0 AND count(*) = count(DISTINCT " +
         CASE
             WHEN size(alias_names) > 1
             THEN "[" + apoc.text.join(alias_names, ", ") + "]"
             ELSE head(alias_names)
         END +
         ") AND count(*) = count(CASE WHEN all(v IN [" + apoc.text.join(alias_names, ", ") +
         "] WHERE v IS NOT NULL) THEN 1 END) AS is_unique" AS q
    CALL apoc.cypher.run(q, {}) YIELD value
    RETURN key_attrs, value.is_unique AS is_unique, value.n AS n
    """

    with driver.session() as session:
        res = session.run(query, {"key_sets": key_sets, "match_block": match_block})
        rows = list(res)
    results = {tuple(r["key_attrs"]): r["is_unique"] for r in rows}
    # the match is the same for all candidates: n is identical
    n_rows = rows[0]["n"] if rows else None
    if filters:
        qf.dbg(f"APOC {pattern_father}: {sum(bool(v) for v in results.values())}/{len(results)} "
               f"unique candidates ({n_rows} rows)", level=2)
    return results, n_rows


