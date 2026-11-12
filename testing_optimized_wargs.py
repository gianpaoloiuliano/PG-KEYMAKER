import argparse
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
import sys

typology = "other_shapes"


def result_path(nomedb, numtest, alg, tree):
    """Path of a test's result file (used both for writing and for resume)."""
    if tree:
        return os.path.join(f"./evaluation/scalability/pgkeymaker_{alg}/{typology}",
                            f"results_PGKEYMAKER_{alg}_{nomedb}_{numtest}.txt")
    return os.path.join(f"./evaluation/scalability/ablation/pgkeymaker_{alg}_notree",
                        f"results_PGKEYMAKER-NOTREE_{nomedb}_{numtest}.txt")


def is_done(path):
    """True if the file exists and is complete (the last non-empty line is 'Max memory used')."""
    try:
        with open(path, encoding="utf-8") as f:
            lines = [l for l in f.read().splitlines() if l.strip()]
        return bool(lines) and lines[-1].startswith("Max memory used")
    except OSError:
        return False

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

        LOG_FILE = result_path(nomedb, numtest, alg, True)
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

        with open(LOG_FILE + ".tmp", "w", encoding="utf-8") as f:

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

        os.replace(LOG_FILE + ".tmp", LOG_FILE)
        print(f"\n📄 Results file saved to:\n{LOG_FILE}\n")

    else:
        LOG_FILE = result_path(nomedb, numtest, alg, False)
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

        with open(LOG_FILE + ".tmp", "w", encoding="utf-8") as f:

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

        os.replace(LOG_FILE + ".tmp", LOG_FILE)
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

    # driver closed in main

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

    # driver closed in main


typology = "other_shapes"


def load_patterns(path):
    """
    Loads the patterns from a file.
      - .json: list of strings or of [pattern, filters] pairs
      - .txt : one pattern per line, optional filters after '||' (empty lines and those starting with '#' are ignored)
    """
    import json
    with open(path, encoding="utf-8") as f:
        if path.lower().endswith(".json"):
            raw = json.load(f)
            return [tuple(p) if isinstance(p, list) else (p, "") for p in raw]
        out = []
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            pat, _, flt = line.partition("||")
            out.append((pat.strip(), flt.strip()))
        return out


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("true", "1", "yes", "y", "t"):
        return True
    if v.lower() in ("false", "0", "no", "n", "f"):
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {v}")


def main():
    global typology
    parser = argparse.ArgumentParser(description="PG-KEYMAKER: run experiments from the command line")
    parser.add_argument("--db", "--nomedb", dest="nomedb", required=True, help="dataset name (used in the result file names)")
    parser.add_argument("--numtest", type=int, required=True, help="number of the first test (incremented for each pattern)")
    parser.add_argument("--tree", type=str2bool, default=True, help="true = with tree, false = notree (default: true)")
    parser.add_argument("--ucc-alg", dest="ucc_alg", default="hyucc", help="UCC algorithm, e.g. hyucc, hpivalid (default: hyucc)")
    parser.add_argument("--patterns-file", help="a .json or .txt file with the patterns")
    parser.add_argument("--pattern", action="append", default=[], help="inline pattern (repeatable)")
    parser.add_argument("--filters", action="append", default=[], help="filters for the corresponding --pattern (same order)")
    parser.add_argument("--lenpattern", default="len_2")
    parser.add_argument("--typology", default="other_shapes", help="results subfolder (only with tree)")
    parser.add_argument("--database", default=None, help="Neo4j database on which to run the queries (default: the default one)")
    parser.add_argument("--skip-existing", dest="skip_existing", type=str2bool, default=True,
                        help="true = skip tests whose result file is already complete (resume, default); false = redo everything")
    parser.add_argument("--check-only", dest="check_only", action="store_true",
                        help="does nothing: exit 0 if all tests are already done, 1 if some are missing")
    parser.add_argument("--uri", default="neo4j://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="gianpaolo")
    args = parser.parse_args()

    patterns = []
    if args.patterns_file:
        patterns += load_patterns(args.patterns_file)
    for i, p in enumerate(args.pattern):
        patterns.append((p, args.filters[i] if i < len(args.filters) else ""))
    if not patterns:
        parser.error("at least one pattern is required (--patterns-file or --pattern)")

    typology = args.typology

    if args.check_only:
        pending = [args.numtest + i for i in range(len(patterns))
                   if not is_done(result_path(args.nomedb, args.numtest + i, args.ucc_alg, args.tree))]
        print(f"missing tests: {pending}" if pending else "all tests already done")
        sys.exit(1 if pending else 0)

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    if args.database:
        _session = driver.session
        driver.session = lambda **kw: _session(**{"database": args.database, **kw})
    numtest = args.numtest
    try:
        for pattern, filters_str in patterns:
            if args.skip_existing and is_done(result_path(args.nomedb, numtest, args.ucc_alg, args.tree)):
                print(f"[resume] test {numtest} already complete, skipping", flush=True)
                numtest += 1
                continue
            if filters_str:
                qf.dbg(f"filters: {filters_str}")
            if args.tree:
                print("launching with tree")
                launch_pgkeymaker(pattern, driver, args.nomedb, numtest, args.lenpattern, args.ucc_alg, args.tree, filters_str)
            else:
                print("launching without tree")
                launch_pgkeymaker_notree(pattern, driver, args.nomedb, numtest, args.lenpattern, args.ucc_alg, args.tree, filters_str)
            numtest += 1
    finally:
        driver.close()


if __name__ == "__main__":
    main()
