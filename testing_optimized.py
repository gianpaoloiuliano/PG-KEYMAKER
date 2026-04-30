import subpattern_extractor as sub_ext
import neo4j_querying_module as n4j
import PG_K_validator as validator
import metanome_ucc as ucc
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

    def set_times(self, ex_leaf,ex_external, ex_case1, ex_case2, ex_case3):
        self.ex_leaf = float(ex_leaf)
        self.ex_external = float(ex_external)
        self.ex_case1 = float(ex_case1)
        self.ex_case2 = float(ex_case2)
        self.ex_case3 = float(ex_case3)

    def get_times(self):
        return (
            self.ex_leaf,
            self.ex_external,
            self.ex_case1,
            self.ex_case2,
            self.ex_case3
        )

    def __repr__(self):
        return (
            f"ExecutionTimes(ex_leaf={self.ex_leaf}, "
            f"ex_case1={self.ex_case1}, "
            f"ex_case2={self.ex_case2}, "
            f"ex_case3={self.ex_case3})"
        )

def save_results_from_graph(G, nomedb, numtest, lenpattern, elapsed_time, end_memory, max_memory,tempi,tree,alg):
    if tree:
        """
        Estrae dal grafo G tutti i nodi che contengono:
            - label   → pattern o subpattern
            - sub_key → lista UCC trovate
        e salva un file CSV usando pandas, compatibile con quello della strategia HyUCC.
        """

        #LOG_DIR = f"./evaluation/scalability/pgkeymaker/{lenpattern}"
        LOG_DIR = f"./evaluation/scalability/pgkeymaker_{alg}/stacked"
        os.makedirs(LOG_DIR, exist_ok=True)

        LOG_FILE = os.path.join(
            LOG_DIR,
            f"results_PGKEYMAKER_{alg}_{nomedb}_{numtest}.txt"
        )

        with open(LOG_FILE, "w", encoding="utf-8") as f:

            # intestazione come HyUCC
            f.write("pattern;keys found\n\n")

            # ciclo su ogni nodo del grafo
            for node, attr in G.nodes(data=True):

                if "label" not in attr:
                    continue

                # valore di default
                pattern = attr.get("pattern")

                # se è presente info_for_ext_match, usa quello al posto del pattern
                info_ext = attr.get("info_for_ext_match")
                if info_ext:
                    pattern = info_ext

                keys = attr.get("sub_key", [])

                # trasforma in stringa leggibile
                keys_str = str(keys)

                f.write(f"{pattern};{keys_str}\n")

            # riepilogo finale
            f.write("\n")
            f.write(f"execution time for tree and leaves: {tempi.ex_leaf:.2f} s\n")
            f.write(f"execution time for external keys: {tempi.ex_external:.2f} s\n")
            f.write(f"execution time for case 1: {tempi.ex_case1:.2f} s\n")
            f.write(f"execution time for case 2: {tempi.ex_case2:.2f} s\n")
            f.write(f"execution time for case 3: {tempi.ex_case3:.2f} s\n")
            f.write(f"total execution time: {elapsed_time:.2f} s\n")
            #f.write(f"Memory: {end_memory:.2f} MB\n")
            f.write(f"Max memory used: {max_memory:.2f} MB\n")

        print(f"\n📄 File risultati salvato in:\n{LOG_FILE}\n")

    else:
        # LOG_DIR = f"./evaluation/scalability/pgkeymaker/{lenpattern}"
        LOG_DIR = f"./evaluation/scalability/ablation/pgkeymaker_{alg}_notree"
        os.makedirs(LOG_DIR, exist_ok=True)

        LOG_FILE = os.path.join(
            LOG_DIR,
            f"results_PGKEYMAKER-NOTREE_{nomedb}_{numtest}.txt"
        )

        with open(LOG_FILE, "w", encoding="utf-8") as f:

            # intestazione come HyUCC
            f.write("pattern;keys found\n\n")

            # ciclo su ogni nodo del grafo
            for node, attr in G.nodes(data=True):

                if "label" not in attr:
                    continue

                # valore di default
                pattern = attr.get("pattern")

                # se è presente info_for_ext_match, usa quello al posto del pattern
                info_ext = attr.get("info_for_ext_match")
                if info_ext:
                    pattern = info_ext

                keys = attr.get("sub_key", [])

                # trasforma in stringa leggibile
                keys_str = str(keys)

                f.write(f"{pattern};{keys_str}\n")

            # riepilogo finale
            f.write("\n")
            f.write(f"execution time for tree and leaves: {tempi.ex_leaf:.2f} s\n")
            f.write(f"execution time for external keys: {tempi.ex_external:.2f} s\n")
            f.write(f"execution time for case 1: {tempi.ex_case1:.2f} s\n")
            f.write(f"execution time for case 2: {tempi.ex_case2:.2f} s\n")
            f.write(f"execution time for case 3: {tempi.ex_case3:.2f} s\n")
            f.write(f"total execution time: {elapsed_time:.2f} s\n")
            # f.write(f"Memory: {end_memory:.2f} MB\n")
            f.write(f"Max memory used: {max_memory:.2f} MB\n")

        print(f"\n📄 File risultati salvato in:\n{LOG_FILE}\n")

def launch_pgkeymaker(pattern,driver,nomedb,numtest,lenpattern,ucc_alg,tree):

    # Forza UTF-8 e aggiungi Java al PATH della sessione Python
    os.environ["PYTHONIOENCODING"] = "utf-8"
    java_path = r"C:\Program Files\Java\jdk-23\bin"
    os.environ["PATH"] = java_path + ";" + os.environ.get("PATH", "")
    tempi = ExecutionTimes()

    #pattern = "(persona:Persona)–[studia:STUDIA_A]-(universita:Universita)–[hasede:HA_SEDE_IN]->(citta:Citta)"
    minimal = True


    scope_freq = n4j.count_pattern_occurrences(pattern,driver)
    #print(scope_freq)

    limit_occurences = 1

    if scope_freq < limit_occurences:
        #print("variabile è true, quindi sono poche occorrenze e posso controllare direttamente")#fai calcolo diretto
        keys = ucc.compute_keys(df_complete_pattern)
        print(keys)
    else:




        #🕒 Avvio misurazione tempo e memoria
        start_time = time.time()
        process = psutil.Process(os.getpid())
        start_memory = process.memory_info().rss / (1024 ** 2)  # in MB
        max_memory = start_memory
        G = sub_ext.build_pattern_graph(pattern)
        # -----------------------------
        # 🕒 MISURAZIONE ex_leaf
        # -----------------------------
        start_leaf_time = time.time()
        #G = sub_ext.build_pattern_graph(pattern)
        analyzed = {}
        element_view = nx.subgraph_view(G,filter_node=lambda n: G.nodes[n].get("label") in ("node", "edge"))
        #print("element_view",element_view.nodes(data=True))
        for leaf in element_view.nodes:
            cur_label = element_view.nodes[leaf].get("original_label")
            print("pattern in esame",element_view.nodes[leaf].get("pattern"))
            if cur_label not in analyzed:
                G = n4j.matching_pattern_in_graph(G,leaf,element_view.nodes[leaf].get("label"),driver,ucc_alg)  # querying the graph
                analyzed[cur_label] = element_view.nodes[leaf].get("pattern")
                G.nodes[leaf]["visited"] = True
                #print(analyzed)
            else:
                src_node = analyzed[cur_label]
                for key in ("external_key", "info_for_ext_match"):
                    value = G.nodes[src_node].get(key)
                    if isinstance(value, list):
                        G.nodes[leaf][key] = list(value)
                    else:
                        G.nodes[leaf][key] = value
                # copia sub_key con alias aggiornato
                src_alias = G.nodes[src_node]["alias"]
                dst_alias = G.nodes[leaf]["alias"]

                G.nodes[leaf]["sub_key"] = [
                    [item.replace(src_alias + ".", dst_alias + ".") for item in sublist]
                    for sublist in G.nodes[src_node]["sub_key"]
                ]

                # marca come visitato
                #print("aggiornato:",G.nodes[leaf])
                #print("sorgente:", G.nodes[src_node])

        tempi.ex_leaf = round(time.time() - start_leaf_time,2)
        print(f"⏱️ Tempo ex_leaf: {tempi.ex_leaf:.2f} secondi")
        G,tempi = validator.check_keys_all_leaf(G,driver,tempi,ucc_alg)
        # ⏱️ calcolo tempo ex_leaf

        #nodi_triple = [n for n, attr in G.nodes(data=True) if attr.get("label") == "tripla"]
        #nodi_subpattern = [n for n, attr in G.nodes(data=True) if attr.get("label") == "original_pattern"]

        G,tempi = validator.compute_keys_by_structure(G,minimal,driver,tempi,ucc_alg)
        print("ex leaves",tempi.ex_leaf)
        print("ex external", tempi.ex_external)
        print("Case 1:", tempi.ex_case1)
        print("Case 2:", tempi.ex_case2)
        print("Case 3:", tempi.ex_case3)

        # 🧮 Calcolo finale di tempo e memoria
        end_time = time.time()
        end_memory = process.memory_info().rss / (1024 ** 2)

        elapsed_time = end_time - start_time
        total_memory_used = end_memory - start_memory

        print("\n=== RISULTATI COMPLESSIVI ===")
        print(f"⏱️  Tempo totale esecuzione: {elapsed_time:.2f} secondi")
        print(f"💾 Memoria finale: {end_memory:.2f} MB")
        print(f"📈 Memoria massima utilizzata: {max_memory:.2f} MB")

        #save_results_from_graph(G, nomedb, numtest,lenpattern, elapsed_time, end_memory, max_memory,tempi,tree=True)

    driver.close()

def launch_pgkeymaker_notree(pattern,driver,nomedb,numtest,lenpattern,ucc_alg,tree):

    # Forza UTF-8 e aggiungi Java al PATH della sessione Python
    os.environ["PYTHONIOENCODING"] = "utf-8"
    java_path = r"C:\Program Files\Java\jdk-23\bin"
    os.environ["PATH"] = java_path + ";" + os.environ.get("PATH", "")
    tempi = ExecutionTimes()

    #pattern = "(persona:Persona)–[studia:STUDIA_A]-(universita:Universita)–[hasede:HA_SEDE_IN]->(citta:Citta)"
    minimal = True


    scope_freq = n4j.count_pattern_occurrences(pattern,driver)
    #print(scope_freq)

    limit_occurences = 1

    if scope_freq < limit_occurences:
        #print("variabile è true, quindi sono poche occorrenze e posso controllare direttamente")#fai calcolo diretto
        keys = ucc.compute_keys(df_complete_pattern)
        print(keys)
    else:
        #🕒 Avvio misurazione tempo e memoria
        start_time = time.time()
        process = psutil.Process(os.getpid())
        start_memory = process.memory_info().rss / (1024 ** 2)  # in MB
        max_memory = start_memory
        G = sub_ext.build_pattern_graph(pattern)
        # -----------------------------
        # 🕒 MISURAZIONE ex_leaf
        # -----------------------------
        start_leaf_time = time.time()
        #G = sub_ext.build_pattern_graph(pattern)
        analyzed = {}
        element_view = nx.subgraph_view(G,filter_node=lambda n: G.nodes[n].get("label") in ("node", "edge"))
        #print("element_view",element_view.nodes(data=True))
        for leaf in element_view.nodes:
            cur_label = element_view.nodes[leaf].get("original_label")
            print("pattern in esame",element_view.nodes[leaf].get("pattern"))
            if cur_label not in analyzed:
                G = n4j.matching_pattern_in_graph(G,leaf,element_view.nodes[leaf].get("label"),driver,ucc_alg)  # querying the graph
                print("sono in not analyzed")
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
                # copia sub_key con alias aggiornato
                src_alias = G.nodes[src_node]["alias"]
                dst_alias = G.nodes[leaf]["alias"]

                G.nodes[leaf]["sub_key"] = [
                    [item.replace(src_alias + ".", dst_alias + ".") for item in sublist]
                    for sublist in G.nodes[src_node]["sub_key"]
                ]

                # marca come visitato
                #print("aggiornato:",G.nodes[leaf])
                #print("sorgente:", G.nodes[src_node])
        '''
        tempi.ex_leaf = round(time.time() - start_leaf_time,2)
        print(f"⏱️ Tempo ex_leaf: {tempi.ex_leaf:.2f} secondi")
        G,tempi = validator.check_keys_all_leaf_notree(G,driver,tempi,ucc_alg)
        # ⏱️ calcolo tempo ex_leaf

        G,tempi = validator.compute_keys_by_structure(G,minimal,driver,tempi,ucc_alg)
        print("ex leaves",tempi.ex_leaf)
        print("ex external", tempi.ex_external)
        print("Case 1:", tempi.ex_case1)
        print("Case 2:", tempi.ex_case2)
        print("Case 3:", tempi.ex_case3)

        # 🧮 Calcolo finale di tempo e memoria
        end_time = time.time()
        end_memory = process.memory_info().rss / (1024 ** 2)

        elapsed_time = end_time - start_time
        total_memory_used = end_memory - start_memory

        print("\n=== RISULTATI COMPLESSIVI ===")
        print(f"⏱️  Tempo totale esecuzione: {elapsed_time:.2f} secondi")
        print(f"💾 Memoria finale: {end_memory:.2f} MB")
        print(f"📈 Memoria massima utilizzata: {max_memory:.2f} MB")

        save_results_from_graph(G, nomedb, numtest,lenpattern, elapsed_time, end_memory, max_memory,tempi,tree,ucc_alg)

    driver.close()


# Configura la connessione a Neo4j
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "gianpaolo")
# Connessione al database Neo4j
driver = GraphDatabase.driver(URI, auth=AUTH)
ucc_alg = "hpivalid"
tree = True
# ------------------------------------------------------------
# ✅ ESEMPIO DI USO
# ------------------------------------------------------------
nomedb="synthea_400"
numtest = 4

lenpattern = "len_2"

'''
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

#finbench
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

'''
for pattern in patterns:
    if tree:
        print("lancio con albero")
        launch_pgkeymaker(pattern,driver,nomedb,numtest,lenpattern,ucc_alg,tree)
        numtest +=1
    else:
        print("lancio senza albero")
        launch_pgkeymaker_notree(pattern,driver,nomedb,numtest,lenpattern,ucc_alg,tree)
        numtest +=1
