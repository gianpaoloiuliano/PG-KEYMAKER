import subpattern_extractor_v2 as sub_ext
import neo4j_querying_module as n4j
import PG_K_validator as validator
import metanome_ucc as ucc
import query_filters as qf
import time
import psutil
from neo4j import GraphDatabase
import networkx as nx
import os

class ExecutionTimes:
    def __init__(self, ex_leaf=0.0, ex_external=0.0, ex_case1=0.0, ex_case2=0.0, ex_case3=0.0):
        self.ex_leaf = float(ex_leaf)
        self.ex_external = float(ex_external)
        self.ex_case1 = float(ex_case1)
        self.ex_case2 = float(ex_case2)
        self.ex_case3 = float(ex_case3)


    def __repr__(self):
        return (
            f"ExecutionTimes(ex_leaf={self.ex_leaf}, "
            f"ex_case1={self.ex_case1}, "
            f"ex_case2={self.ex_case2}, "
            f"ex_case3={self.ex_case3})"
        )

def save_results_from_graph(G, nomedb, numtest, lenpattern, elapsed_time, end_memory, max_memory,tempi,tree,alg,filters_str=""):
    if tree:
        """
        Extracts from graph G all the nodes that contain:
            - label   → pattern or subpattern
            - sub_key → list of UCCs found
        and saves a CSV file using pandas, compatible with that of the HyUCC strategy.
        """

        #LOG_DIR = f"./evaluation/scalability/pgkeymaker/{lenpattern}"
        LOG_DIR = f"./evaluation/scalability/pgkeymaker_{alg}/{typology}"
        os.makedirs(LOG_DIR, exist_ok=True)

        LOG_FILE = os.path.join(
            LOG_DIR,
            f"results_PGKEYMAKER_{alg}_{nomedb}_{numtest}.txt"
        )

        with open(LOG_FILE, "w", encoding="utf-8") as f:

            # filters used (only if present, so files without filters stay unchanged)
            if filters_str:
                f.write(f"filters: {filters_str}\n")
            # header as in HyUCC
            f.write("pattern;keys found\n\n")

            # loop over every node of the graph
            for node, attr in G.nodes(data=True):

                if "label" not in attr:
                    continue

                # default value
                pattern = attr.get("pattern")

                # if info_for_ext_match is present, use it instead of the pattern
                info_ext = attr.get("info_for_ext_match")
                if info_ext:
                    pattern = info_ext

                keys = attr.get("sub_key", [])

                # convert to a readable string
                keys_str = str(keys)

                f.write(f"{pattern};{keys_str}\n")

            # final summary
            f.write("\n")
            f.write(f"execution time for tree and leaves: {tempi.ex_leaf:.2f} s\n")
            f.write(f"execution time for external keys: {tempi.ex_external:.2f} s\n")
            f.write(f"execution time for case 1: {tempi.ex_case1:.2f} s\n")
            f.write(f"execution time for case 2: {tempi.ex_case2:.2f} s\n")
            f.write(f"execution time for case 3: {tempi.ex_case3:.2f} s\n")
            f.write(f"total execution time: {elapsed_time:.2f} s\n")
            #f.write(f"Memory: {end_memory:.2f} MB\n")
            f.write(f"Max memory used: {max_memory:.2f} MB\n")

        print(f"\n📄 Results file saved to:\n{LOG_FILE}\n")

    else:
        # LOG_DIR = f"./evaluation/scalability/pgkeymaker/{lenpattern}"
        LOG_DIR = f"./evaluation/scalability/ablation/pgkeymaker_{alg}_notree/{typology}"
        os.makedirs(LOG_DIR, exist_ok=True)

        LOG_FILE = os.path.join(
            LOG_DIR,
            f"results_PGKEYMAKER-NOTREE_{nomedb}_{numtest}.txt"
        )

        with open(LOG_FILE, "w", encoding="utf-8") as f:

            # filters used (only if present, so files without filters stay unchanged)
            if filters_str:
                f.write(f"filters: {filters_str}\n")
            # header as in HyUCC
            f.write("pattern;keys found\n\n")

            # loop over every node of the graph
            for node, attr in G.nodes(data=True):

                if "label" not in attr:
                    continue

                # default value
                pattern = attr.get("pattern")

                # if info_for_ext_match is present, use it instead of the pattern
                info_ext = attr.get("info_for_ext_match")
                if info_ext:
                    pattern = info_ext

                keys = attr.get("sub_key", [])

                # convert to a readable string
                keys_str = str(keys)

                f.write(f"{pattern};{keys_str}\n")

            # final summary
            f.write("\n")
            f.write(f"execution time for tree and leaves: {tempi.ex_leaf:.2f} s\n")
            f.write(f"execution time for external keys: {tempi.ex_external:.2f} s\n")
            f.write(f"execution time for case 1: {tempi.ex_case1:.2f} s\n")
            f.write(f"execution time for case 2: {tempi.ex_case2:.2f} s\n")
            f.write(f"execution time for case 3: {tempi.ex_case3:.2f} s\n")
            f.write(f"total execution time: {elapsed_time:.2f} s\n")
            # f.write(f"Memory: {end_memory:.2f} MB\n")
            f.write(f"Max memory used: {max_memory:.2f} MB\n")

        print(f"\n📄 Results file saved to:\n{LOG_FILE}\n")

def launch_pgkeymaker(pattern,driver,nomedb,numtest,lenpattern,ucc_alg,tree,filters_str=""):

    # Force UTF-8 and add Java to the PATH of the Python session
    os.environ["PYTHONIOENCODING"] = "utf-8"
    java_path = r"C:\Program Files\Java\jdk-23\bin"
    os.environ["PATH"] = java_path + ";" + os.environ.get("PATH", "")
    tempi = ExecutionTimes()
    minimal = True


    filters = qf.parse_filters(filters_str, pattern)
    scope_freq = n4j.count_pattern_occurrences(pattern,driver,filters)
    if filters:
        freq_all = n4j.count_pattern_occurrences(pattern,driver)
        qf.dbg(f"pattern occurrences: {scope_freq} with filters / {freq_all} without filters")
    #print(scope_freq)

    limit_occurences = 0

    if scope_freq < limit_occurences:
        #print("variable is true, so there are few occurrences and I can check directly")#do direct computation
        keys = ucc.compute_keys(df_complete_pattern)
        print(keys)
    else:


        #🕒 Start time and memory measurement
        start_time = time.time()
        process = psutil.Process(os.getpid())
        start_memory = process.memory_info().rss / (1024 ** 2)  # in MB
        max_memory = start_memory
        G = sub_ext.build_pattern_graph(pattern, filters)
        # -----------------------------
        # 🕒 ex_leaf MEASUREMENT
        # -----------------------------
        start_leaf_time = time.time()
        #G = sub_ext.build_pattern_graph(pattern)
        analyzed = {}
        element_view = nx.subgraph_view(G,filter_node=lambda n: G.nodes[n].get("label") in ("node", "edge"))
        #print("element_view",element_view.nodes(data=True))
        for leaf in element_view.nodes:
            cur_label = element_view.nodes[leaf].get("original_label")
            print("pattern under examination",element_view.nodes[leaf].get("pattern") + qf.filters_label(G.nodes[leaf].get("filters")))
            # two leaves with the same label share keys only if they have the same filters
            leaf_sig = qf.filter_signature(G.nodes[leaf].get("filters"), G.nodes[leaf]["alias"])
            cache_key = (cur_label, leaf_sig)
            if leaf_sig:
                qf.dbg(f"leaf cache {leaf}: {'HIT' if cache_key in analyzed else 'MISS'}", level=2)
            if cache_key not in analyzed:
                G = n4j.matching_pattern_in_graph(G,leaf,element_view.nodes[leaf].get("label"),driver,ucc_alg)  # querying the graph
                analyzed[cache_key] = element_view.nodes[leaf].get("pattern")
                G.nodes[leaf]["visited"] = True
                #print(analyzed)
            else:
                src_node = analyzed[cache_key]
                for key in ("external_key", "info_for_ext_match"):
                    value = G.nodes[src_node].get(key)
                    if isinstance(value, list):
                        G.nodes[leaf][key] = list(value)
                    else:
                        G.nodes[leaf][key] = value
                # copy sub_key with updated alias
                src_alias = G.nodes[src_node]["alias"]
                dst_alias = G.nodes[leaf]["alias"]

                G.nodes[leaf]["sub_key"] = [
                    [item.replace(src_alias + ".", dst_alias + ".") for item in sublist]
                    for sublist in G.nodes[src_node]["sub_key"]
                ]

                # mark as visited
                #print("updated:",G.nodes[leaf])
                #print("source:", G.nodes[src_node])

        tempi.ex_leaf = round(time.time() - start_leaf_time,2)
        print(f"⏱️ ex_leaf time: {tempi.ex_leaf:.2f} seconds")
        G,tempi = validator.check_keys_all_leaf(G,driver,tempi,ucc_alg)
        # ⏱️ ex_leaf time computation

        #nodi_triple = [n for n, attr in G.nodes(data=True) if attr.get("label") == "tripla"]
        #nodi_subpattern = [n for n, attr in G.nodes(data=True) if attr.get("label") == "original_pattern"]

        G,tempi = validator.compute_keys_by_structure(G,minimal,driver,tempi,ucc_alg)
        print("ex leaves",tempi.ex_leaf)
        print("ex external", tempi.ex_external)
        print("Case 1:", tempi.ex_case1)
        print("Case 2:", tempi.ex_case2)
        print("Case 3:", tempi.ex_case3)

        # 🧮 Final computation of time and memory
        end_time = time.time()
        end_memory = process.memory_info().rss / (1024 ** 2)

        elapsed_time = end_time - start_time
        total_memory_used = end_memory - start_memory

        print("\n=== OVERALL RESULTS ===")
        print(f"⏱️  Total execution time: {elapsed_time:.2f} seconds")
        print(f"💾 Final memory: {end_memory:.2f} MB")
        print(f"📈 Max memory used: {max_memory:.2f} MB")
        print(ucc_alg)

        save_results_from_graph(G, nomedb, numtest,lenpattern, elapsed_time, end_memory, max_memory,tempi,tree,ucc_alg,filters_str)

    driver.close()

def launch_pgkeymaker_notree(pattern,driver,nomedb,numtest,lenpattern,ucc_alg,tree,filters_str=""):

    # Force UTF-8 and add Java to the PATH of the Python session
    os.environ["PYTHONIOENCODING"] = "utf-8"
    java_path = r"C:\Program Files\Java\jdk-23\bin"
    os.environ["PATH"] = java_path + ";" + os.environ.get("PATH", "")
    tempi = ExecutionTimes()

    #pattern = "(persona:Persona)–[studia:STUDIA_A]-(universita:Universita)–[hasede:HA_SEDE_IN]->(citta:Citta)"
    minimal = True


    filters = qf.parse_filters(filters_str, pattern)
    scope_freq = n4j.count_pattern_occurrences(pattern,driver,filters)
    if filters:
        freq_all = n4j.count_pattern_occurrences(pattern,driver)
        qf.dbg(f"pattern occurrences: {scope_freq} with filters / {freq_all} without filters")
    #print(scope_freq)

    limit_occurences = 1

    if scope_freq < limit_occurences:
        #print("variable is true, so there are few occurrences and I can check directly")#do direct computation
        keys = ucc.compute_keys(df_complete_pattern)
        print(keys)
    else:
        #🕒 Start time and memory measurement
        start_time = time.time()
        process = psutil.Process(os.getpid())
        start_memory = process.memory_info().rss / (1024 ** 2)  # in MB
        max_memory = start_memory
        G = sub_ext.build_pattern_graph(pattern, filters)
        # -----------------------------
        # 🕒 ex_leaf MEASUREMENT
        # -----------------------------
        start_leaf_time = time.time()
        #G = sub_ext.build_pattern_graph(pattern)
        analyzed = {}
        element_view = nx.subgraph_view(G,filter_node=lambda n: G.nodes[n].get("label") in ("node", "edge"))
        #print("element_view",element_view.nodes(data=True))
        for leaf in element_view.nodes:
            cur_label = element_view.nodes[leaf].get("original_label")
            print("pattern under examination",element_view.nodes[leaf].get("pattern") + qf.filters_label(G.nodes[leaf].get("filters")))
            if cur_label not in analyzed:
                G = n4j.matching_pattern_in_graph(G,leaf,element_view.nodes[leaf].get("label"),driver,ucc_alg)  # querying the graph
                print("in not analyzed branch")
                #analyzed[cur_label] = element_view.nodes[leaf].get("pattern")
                #G.nodes[leaf]["visited"] = True
                #print(analyzed)
        '''        
            else:
                src_node = analyzed[cur_label]
                for key in ("external_key", "info_for_ext_match"):
                    value = G.nodes[src_node].get(key)
                    if isinstance(value, list):
                        G.nodes[leaf][key] = list(value)
                    else:
                        G.nodes[leaf][key] = value
                # copy sub_key with updated alias
                src_alias = G.nodes[src_node]["alias"]
                dst_alias = G.nodes[leaf]["alias"]

                G.nodes[leaf]["sub_key"] = [
                    [item.replace(src_alias + ".", dst_alias + ".") for item in sublist]
                    for sublist in G.nodes[src_node]["sub_key"]
                ]

                # mark as visited
                #print("updated:",G.nodes[leaf])
                #print("source:", G.nodes[src_node])
        '''
        tempi.ex_leaf = round(time.time() - start_leaf_time,2)
        print(f"⏱️ ex_leaf time: {tempi.ex_leaf:.2f} seconds")
        G,tempi = validator.check_keys_all_leaf_notree(G,driver,tempi,ucc_alg)
        # ⏱️ ex_leaf time computation

        G,tempi = validator.compute_keys_by_structure(G,minimal,driver,tempi,ucc_alg)
        print("ex leaves",tempi.ex_leaf)
        print("ex external", tempi.ex_external)
        print("Case 1:", tempi.ex_case1)
        print("Case 2:", tempi.ex_case2)
        print("Case 3:", tempi.ex_case3)

        # 🧮 Final computation of time and memory
        end_time = time.time()
        end_memory = process.memory_info().rss / (1024 ** 2)

        elapsed_time = end_time - start_time
        total_memory_used = end_memory - start_memory

        print("\n=== OVERALL RESULTS ===")
        print(f"⏱️  Total execution time: {elapsed_time:.2f} seconds")
        print(f"💾 Final memory: {end_memory:.2f} MB")
        print(f"📈 Max memory used: {max_memory:.2f} MB")

        save_results_from_graph(G, nomedb, numtest,lenpattern, elapsed_time, end_memory, max_memory,tempi,tree,ucc_alg,filters_str)

    driver.close()


# Configure the Neo4j connection
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "gianpaolo")
# Connection to the Neo4j database
driver = GraphDatabase.driver(URI, auth=AUTH)
ucc_alg = "hyucc"
tree = True
# ------------------------------------------------------------
# ✅ USAGE EXAMPLE
# ------------------------------------------------------------

nomedb="finbench_1perc"
numtest = 99
lenpattern = "len_2"
typology = "meaningfull"

patterns = [
    ("(src1:Account)-[edge1:Transfer]->(mid:Account)-[edge2:Withdraw]->(dstCard:Account)",
     "datetime('2021-01-01T00:00:00Z') < edge1.createTime < datetime('2022-01-01T00:00:00Z') "
     "AND datetime('2021-01-01T00:00:00Z') < edge2.createTime < datetime('2022-01-01T00:00:00Z') "
     "AND edge1.amount > 50 AND edge2.amount > 100"),
]
'''
#with aggregation
patterns = [
    # "What comorbidities are associated with a particular Condition?"
    ("(c1:Condition)<-[hc1:HAS_CONDITION]-(en:Encounter)-[hc2:HAS_CONDITION]->(c2:Condition)",
     "c1 <> c2 AND count(DISTINCT c1) >= 40 PER c2"),

    # "Given a Patient what Drugs were PRESCRIBED?"
    ("(p:Patient)-[has:HAS_ENCOUNTER]->(en:Encounter)-[hd:HAS_DRUG]->(dr:Drug)",
     "count(DISTINCT dr) >= 6 PER p"),

    # "Given a Patient what Conditions were DIAGNOSED?"
    ("(p:Patient)-[has:HAS_ENCOUNTER]->(en:Encounter)-[hc:HAS_CONDITION]->(c:Condition)",
     "count(DISTINCT c) >= 19 PER p"),
]
'''

# Optional filters: each element of `patterns` can be a string (no filters)
# or a tuple (pattern, filters). Filters are free-form Cypher, split on the ANDs;
# aggregates must be grouped on a node with "PER <alias>". Example:
# patterns = [
#     ("(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)",
#      "p.GENDER = 'F' AND count(en1) > 5 PER p"),
# ]
'''

# ===================== FRAUD =====================

patterns =[
    ("(tt:Transfer:Transaction)-[to:TO]->(cl:Client)-[pf:PERFORMED]->(ct:CashIn:Transaction)", ""),
    ("(tt:Transfer:Transaction)-[to:TO]->(cl:Client)-[pf:PERFORMED]->(co:CashOut:Transaction)",""),
    ("(tt:Transfer:Transaction)-[to:TO]->(cl:Client)-[pf:PERFORMED]->(pt:Payment:Transaction)",""),
    ("(tt:Transfer:Transaction)-[to:TO]->(cl:Client)-[pf:PERFORMED]->(tt2:Transfer:Transaction)",""),
    ("(cl:Client)-[pf:PERFORMED]->(ct:CashIn:Transaction)-[to:TO]->(m:Merchant)", ""),
    ("(ct:CashIn:Transaction)-[n:NEXT]->(ct2:CashIn:Transaction)-[to:TO]->(m:Merchant)", ""),
    ("(cl:Client)-[pf:PERFORMED]->(ct:CashIn:Transaction)-[n:NEXT]->(ct2:CashIn:Transaction)", ""),
    ("(cl:Client)-[pf:PERFORMED]->(ct:CashOut:Transaction)-[to:TO]->(m:Merchant)", ""),
]

#3 hops fraud
patterns =[
    ("(cl:Client)-[pf:PERFORMED]->(tt2:Transfer:Transaction)-[to:TO]->(cl2:Client)-[pf2:PERFORMED]->(ct:CashIn:Transaction)",""),
    ("(tt:Transfer:Transaction)-[to:TO]->(cl:Client)-[pf:PERFORMED]->(ct:CashIn:Transaction)-[to2:TO]->(m:Merchant)", ""),
]

#4 hops fraud
patterns =[
    ("(n1:Transfer:Transaction)-[rel1:TO]->(n2:Client)-[rel2:PERFORMED]->(n3:Transfer:Transaction)-[rel3:TO]->(n4:Client)-[rel4:PERFORMED]->(n5:CashIn:Transaction)",""),
    ("(n1:Transfer:Transaction)-[rel1:TO]->(n2:Client)-[rel2:PERFORMED]->(n3:Transfer:Transaction)-[rel3:TO]->(n4:Client)-[rel4:PERFORMED]->(n5:CashOut:Transaction)","")
]

#5 hops fraud
patterns =[
    ("(n1:Transfer:Transaction)-[rel1:TO]->(n2:Client)-[rel2:PERFORMED]->(n3:Transfer:Transaction)-[rel3:TO]->(n4:Client)-[rel4:PERFORMED]->(n5:CashIn:Transaction)-[rel5:NEXT]->(n6:CashIn:Transaction)",""),
    ("(n1:Transfer:Transaction)-[rel1:TO]->(n2:Client)-[rel2:PERFORMED]->(n3:Transfer:Transaction)-[rel3:TO]->(n4:Client)-[rel4:PERFORMED]->(n5:CashOut:Transaction)-[rel5:TO]->(n6:Merchant)","")
]

#meaningfull fraud (https://scispace.com/pdf/analysis-of-fraud-controls-using-the-paysim-financial-4dvauv8xq7.pdf)
patterns = [
    # Control 1: threshold on the maximum amount of a single TRANSFER
    # (the paper studies thresholds of 300k, 600k, 900k and 1200k; here the lowest)
    ("(c1:Client)-[perf:PERFORMED]->(tx:Transfer)-[to:TO]->(c2:Client)","tx.amount >= 300000"),

    # Control 2: block accounts that exceed three consecutive transfers
    # at the maximum amount in a short period
    ("(c1:Client)-[perf:PERFORMED]->(tx:Transfer)-[to:TO]->(c2:Client)","tx.amount >= 300000 AND count(DISTINCT tx) > 3 PER c1"),

    # Fraud scheme: transfer to a mule, who then withdraws at a merchant;
    # if the balance exceeds the maximum, multiple mules are used
    ("(victim:Client)-[perf1:PERFORMED]->(t1:Transfer)-[to1:TO]->(mule:Client)-[perf2:PERFORMED]->(t2:CashOut)-[to2:TO]->(m:Merchant)","t1.step <= t2.step AND count(DISTINCT mule) > 1 PER victim"),
]

# ===================== NETWORK =====================
patterns = [
    "(m:Machine)-[r:ROUTES]-(i:Interface)-[e:EXPOSES]-(p:Port)",
    "(a:Application:Process)-[d:DEPENDS_ON]-(se:Service:Process)-[l:LISTENS]-(p:Port)",
    "(m:Machine)-[r:RUNS]-(se:Service:Process)-[l:LISTENS]-(p:Port)",
    "(m:Machine)-[r:RUNS]-(a:Application:Process)-[dep:DEPENDS_ON]-(se:Service:Process)",
    "(ra:Rack)-[h:HOLDS]-(m:Machine)-[r:RUNS]-(se:Service:Process)",
    "(se:Service:Process)-[i:INSTANCE]-(ss:Software:Service)-[v:VERSION]-(ver:Version)",
    "(m:Machine)-[r:RUNS]-(a:Application:Process)-[l:LISTENS]-(p:Port)",
    "(m:Machine)-[r:RUNS]-(o:OS:Process)-[i:INSTANCE]-(ver:Version)",
]

#with aggregation
patterns = [
    # Fault domain: racks hosting many instances of the same application
    ("(ra:Rack)-[h:HOLDS]-(m:Machine)-[r:RUNS]-(a:Application:Process)",
     "a.name = 'crm' AND count(m) > 40 PER ra"),

    # Security audit: obsolete OS versions still installed on many machines
    ("(m:Machine)-[r:RUNS]-(o:OS:Process)-[i:INSTANCE]-(ver:Version)",
     "ver.name IN ['14.04', '7.1', '6-Squeeze'] AND count(m) > 500 PER ver"),

    # Impact of a zone failure: machines in zone 1 with multiple active services
    ("(ra:Rack)-[h:HOLDS]-(m:Machine)-[r:RUNS]-(se:Service:Process)",
     "ra.zone = 1 AND count(se) > 1 PER m"),
]

#3 hops network
patterns = [
    ("(r:Rack)-[h:HOLDS]->(ma:Machine)-[ro:ROUTES]->(in:Interface)-[exp:EXPOSES]->(po:Port)",""),
    ("(r:Rack)-[h:HOLDS]->(ma:Machine)-[ru:RUNS]->(sp:Service:Process)-[li:LISTENS]->(po:Port)",""),
]
#4 hops network
patterns =[
    ("(n1:DataCenter)-[rel1:CONTAINS]->(n2:Rack)-[rel2:HOLDS]->(n3:Machine)-[rel3:ROUTES]->(n4:Interface)-[rel4:EXPOSES]->(n5:Port)",""),
    ("(n1:Rack)-[rel1:HOLDS]->(n2:Machine)-[rel2:RUNS]->(n3:Application:Process)-[rel3:DEPENDS_ON]->(n4:Service:Process)-[rel4:LISTENS]->(n5:Port)","")
]

#5 hops network
patterns =[
    ("(n1:DataCenter)-[rel1:CONTAINS]->(n2:Rack)-[rel2:HOLDS]->(n3:Machine)-[rel3:RUNS]->(n4:Application:Process)-[rel4:DEPENDS_ON]->(n5:Service:Process)-[rel5:LISTENS]->(n6:Port)",""),
    ("(n1:DataCenter)-[rel1:CONTAINS]->(n2:Rack)-[rel2:HOLDS]->(n3:Machine)-[rel3:RUNS]->(n4:Service:Process)-[rel4:INSTANCE]->(n5:Software:Service)-[rel5:VERSION]->(n6:Version)","")
]

# ===================== SYNTHEA ===================== 

#synthea_100
patterns = [
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[nx:NEXT]-(en2:Encounter)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[hd:HAS_DRUG]-(dr:Drug)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hd:HAS_DRUG]-(dr:Drug)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[he:HAS_END]-(en2:Encounter)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[he:HAS_END]-(en3:Encounter)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[nx2:NEXT]-(en3:Encounter)"
]

#synthea_200
patterns = [
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[nx:NEXT]-(en2:Encounter)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[nx2:NEXT]-(en3:Encounter)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[hd:HAS_DRUG]-(dr:Drug)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hd:HAS_DRUG]-(dr:Drug)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[he:HAS_END]-(en2:Encounter)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[he:HAS_END]-(en3:Encounter)",
]

#synthea_300-1000
patterns = [
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[nx:NEXT]-(en2:Encounter)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[nx2:NEXT]-(en3:Encounter)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[hd:HAS_DRUG]-(dr:Drug)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hd:HAS_DRUG]-(dr:Drug)",
    "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[he:HAS_END]-(en2:Encounter)",
    "(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[he:HAS_END]-(en3:Encounter)",
]


#3 hops synthea_1000
patterns = [
    ("(p:Patient)-[has:HAS_ENCOUNTER]->(en:Encounter)-[nxt:NEXT]->(en1:Encounter)-[hasp:HAS_PROCEDURE]->(pro:Procedure)",""),
    ("(en1:Encounter)-[nx:NEXT]->(en2:Encounter)-[nx1:NEXT]->(en3:Encounter)-[hasp:HAS_PROCEDURE]->(pro:Procedure)",""),
]

#4 hops synthea_1000
patterns =[
    ("(n1:Patient)-[rel1:HAS_ENCOUNTER]->(n2:Encounter)-[rel2:NEXT]->(n3:Encounter)-[rel3:NEXT]->(n4:Encounter)-[rel4:HAS_PROCEDURE]->(n5:Procedure)",""),
    ("(n1:Encounter)-[rel1:NEXT]->(n2:Encounter)-[rel2:NEXT]->(n3:Encounter)-[rel3:NEXT]->(n4:Encounter)-[rel4:HAS_PROCEDURE]->(n5:Procedure)","")
]

# ===================== FINBENCH =====================
patterns = [
    "(a:Account)-[tran:Transfer]-(a2:Account)-[wd:Withdraw]-(a3:Account)",
    "(a:Account)-[tran:Transfer]-(a2:Account)-[tran2:Transfer]-(a3:Account)",
    "(a:Account)-[wd:Withdraw]-(a2:Account)-[tran:Transfer]-(a3:Account)",
    "(ln:Loan)-[dep:Deposit]-(a:Account)-[tran:Transfer]-(a2:Account)",
    "(a:Account)-[tran:Transfer]-(a2:Account)-[rep:Repay]-(ln:Loan)",
    "(ln:Loan)-[dep:Deposit]-(a:Account)-[wd:Withdraw]-(a2:Account)",
    "(med:Medium)-[sig:SignIn]-(a:Account)-[wd:Withdraw]-(a2:Account)",
    "(a:Account)-[wd:Withdraw]-(a2:Account)-[rep:Repay]-(ln:Loan)"
]

#meaningfull queries
patterns = [
    # TCR 6 — Withdrawal after Many-to-One transfer
    ("(src1:Account)-[edge1:Transfer]->(mid:Account)-[edge2:Withdraw]->(dstCard:Account)",
     "datetime('2021-01-01T00:00:00Z') < edge1.createTime < datetime('2022-01-01T00:00:00Z') "
     "AND datetime('2021-01-01T00:00:00Z') < edge2.createTime < datetime('2022-01-01T00:00:00Z') "
     "AND edge1.amount > 50 AND edge2.amount > 100"),

    # TCR 1 — Blocked medium related accounts (path of 1 transfer)
    ("(account:Account)-[edge1:Transfer]->(other:Account)<-[edge2:SignIn]-(medium:Medium {isBlocked: true})",
     "datetime('2021-01-01T00:00:00Z') < edge1.createTime < datetime('2022-01-01T00:00:00Z') "
     "AND datetime('2021-01-01T00:00:00Z') < edge2.createTime < datetime('2022-01-01T00:00:00Z')"),

    # TCR 12 — Transfer to company amount statistics
    ("(person:Person)-[edge1:Own]->(pAcc:Account)-[edge2:Transfer]->(compAcc:Account)<-[edge3:Own]-(com:Company)",
     "datetime('2021-01-01T00:00:00Z') < edge2.createTime < datetime('2022-01-01T00:00:00Z')"),
]

#3 hops finbench
patterns = [
    ("(a:Account)-[tran:Transfer]->(a2:Account)-[tran2:Transfer]->(a3:Account)-[wd:Withdraw]->(a4:Account)",""),
    ("(a1:Account)-[wd:Withdraw]->(a2:Account)-[tran:Transfer]->(a3:Account)-[wd2:Withdraw]->(a4:Account)",""),
]

#4 hops finbench
patterns =[
    ("(n1:Account)-[rel1:Withdraw]->(n2:Account)-[rel2:Transfer]->(n3:Account)-[rel3:Transfer]->(n4:Account)-[rel4:Withdraw]->(n5:Account)",""),
    ("(n1:Account)-[rel1:Transfer]->(n2:Account)-[rel2:Transfer]->(n3:Account)-[rel3:Transfer]->(n4:Account)-[rel4:Withdraw]->(n5:Account)","")
]

# 5 hops finbench
patterns = [
    ("(n1:Account)-[rel1:Withdraw]->(n2:Account)-[rel2:Transfer]->(n3:Account)-[rel3:Transfer]->(n4:Account)-[rel4:Transfer]->(n5:Account)-[rel5:Withdraw]->(n6:Account)",""),
    ("(n1:Account)-[rel1:Transfer]->(n2:Account)-[rel2:Transfer]->(n3:Account)-[rel3:Transfer]->(n4:Account)-[rel4:Transfer]->(n5:Account)-[rel5:Withdraw]->(n6:Account)","")
]


'''

for pattern in patterns:
    pattern, filters_str = pattern if isinstance(pattern, tuple) else (pattern, "")
    if filters_str:
        qf.dbg(f"filters: {filters_str}")
    if tree:
        print("launching with tree")
        launch_pgkeymaker(pattern,driver,nomedb,numtest,lenpattern,ucc_alg,tree,filters_str)
        numtest +=1
    else:
        print("launching without tree")
        launch_pgkeymaker_notree(pattern,driver,nomedb,numtest,lenpattern,ucc_alg,tree,filters_str)
        numtest +=1
