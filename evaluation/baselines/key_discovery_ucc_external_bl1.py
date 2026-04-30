from neo4j import GraphDatabase
import pandas as pd
import openclean_metanome.config as config
from openclean_metanome.algorithm.hyucc import hyucc
import os, sys, re, time, psutil

# ------------------------------------------------------------
# ✅ Setup encoding
# ------------------------------------------------------------
os.environ["PYTHONIOENCODING"] = "utf-8"
if sys.platform.startswith("win"):
    os.system("chcp 65001 > nul")

# ------------------------------------------------------------
# ✅ UCC Discovery
# ------------------------------------------------------------
def compute_keys(df):
    env = {config.METANOME_JARPATH: config.JARFILE()}
    return hyucc(df, env=env, verbose=False, max_ucc_size=-1)


# ------------------------------------------------------------
# ✅ Pattern utilities
# ------------------------------------------------------------
def extract_all_valid_subpatterns(pattern):
    parts = pattern.split('-')
    subpatterns = set()

    for part in parts:
        subpatterns.add(part)

    for start in range(0, len(parts), 2):
        for length in range(3, len(parts) - start + 1, 2):
            sub = parts[start:start + length]
            valid = True
            for i, elem in enumerate(sub):
                if i % 2 == 0 and not (elem.startswith('(') and elem.endswith(')')):
                    valid = False
                if i % 2 == 1 and not (elem.startswith('[') and elem.endswith(']')):
                    valid = False
            if valid:
                subpatterns.add('-'.join(sub))

    return list(subpatterns)


def generate_query_from_pattern(pattern_str):
    pattern_str = pattern_str.strip()

    # solo nodo
    if pattern_str.startswith("(") and pattern_str.endswith(")"):
        return f"MATCH {pattern_str} RETURN *"

    # solo relazione
    if pattern_str.startswith("[") and pattern_str.endswith("]"):
        return f"MATCH ()-{pattern_str}-() RETURN {pattern_str[1:pattern_str.find(':')]}"

    # pattern misto
    return f"MATCH {pattern_str} RETURN *"



# ------------------------------------------------------------
# ✅ Neo4j connection
# ------------------------------------------------------------
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "gianpaolo"))


# ------------------------------------------------------------
# ✅ Query runner
# ------------------------------------------------------------
def run_query(tx, query):
    result = tx.run(query)
    rows = []

    for record in result:
        row = {}
        for k, v in record.items():
            if hasattr(v, "_properties"):
                for pk, pv in v._properties.items():
                    row[f"{k}.{pk}"] = pv
            else:
                row[k] = v
        rows.append(row)

    return pd.DataFrame(rows)


# ------------------------------------------------------------
# ✅ Baseline utilities
# ------------------------------------------------------------
def is_single_node_pattern(sub):
    return sub.startswith("(") and sub.endswith(")") and "-" not in sub


def extract_node_label(node_pattern):
    m = re.search(r':(\w+)', node_pattern)
    return m.group(1) if m else node_pattern.strip("()")


def find_neighbors(session, target_label):
    query = f"""
    MATCH (t:{target_label})-[r]-(n)
    RETURN DISTINCT labels(n)[0] AS neighborLabel, type(r) AS relType
    """
    return [(r["neighborLabel"], r["relType"]) for r in session.run(query)]


def build_neighbor_df(session, target_label, neighbor_label, rel_type):
    query = f"""
    MATCH (t:{target_label})-[:{rel_type}]-(n:{neighbor_label})
    RETURN n
    """
    return session.execute_read(run_query, query)


def check_coverage(session, target_label, neighbor_label, rel_type):
    query = f"""
    MATCH (t:{target_label})
    OPTIONAL MATCH (t)-[:{rel_type}]-(n:{neighbor_label})
    WITH count(DISTINCT t) AS total,
         count(DISTINCT CASE WHEN n IS NOT NULL THEN t END) AS covered
    RETURN covered = total AS ok
    """
    return session.run(query).single()["ok"]


def check_one_to_one(session, target_label, neighbor_label, rel_type):
    query = f"""
    MATCH (t:{target_label})-[:{rel_type}]-(n:{neighbor_label})
    WITH n, count(DISTINCT t) AS c
    WHERE c > 1
    RETURN count(n) = 0 AS ok
    """
    return session.run(query).single()["ok"]

# ------------------------------------------------------------
# ✅ Main processing (CON DEBUG)
# ------------------------------------------------------------
def process_pattern(pattern, nomedb, numpattern, lenpattern):

    print(f"\n🚀 Avvio PATTERN #{numpattern}")
    print(f"   Pattern: {pattern}")

    start_time = time.time()
    process = psutil.Process(os.getpid())
    # tempi separati
    internal_time = 0
    external_time = 0

    start_memory = process.memory_info().rss / (1024 ** 2)
    max_memory = start_memory

    LOG_DIR = f"../scalability/ucc_baseline1"
    LOG_FILE = os.path.join(
        LOG_DIR,
        f"results_HyUCC_bs1_{nomedb}_{numpattern}.txt"
    )
    os.makedirs(LOG_DIR, exist_ok=True)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n### PATTERN {pattern}\n")

    subpatterns = extract_all_valid_subpatterns(pattern)
    print(f"   🔹 Subpattern trovati: {len(subpatterns)}")

    with driver.session() as session:
        for i, sub in enumerate(subpatterns, start=1):
            t0 = time.time()
            print(f"      ▶ Subpattern {i}/{len(subpatterns)}: {sub}")

            query = generate_query_from_pattern(sub)
            df = session.execute_read(run_query, query)

            if df.shape[0] < 2 or df.shape[1] < 2:
                print("         ⏭️ Dataset troppo piccolo")
                continue

            # ---- INTERNAL KEYS ----
            print("         🔍 Calcolo chiavi interne...")


            keys = compute_keys(df)
            t1 = time.time()

            internal_time += (t1 - t0)

            current_memory = process.memory_info().rss / (1024 ** 2)
            max_memory = max(max_memory, current_memory)

            if keys:
                print(f"         ✅ INTERNAL_KEYS trovate: {keys}")
                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(f"{sub} - INTERNAL_KEYS {keys}\n")
                continue

            # ---- BASELINE 1 / EXTERNAL KEYS ----
            if not is_single_node_pattern(sub):
                print("         ⏭️ Non single-node pattern")
                continue

            t0 = time.time()
            target_label = extract_node_label(sub)
            neighbors = find_neighbors(session, target_label)

            print(f"         🔗 Vicini trovati: {len(neighbors)}")

            for neighbor_label, rel_type in neighbors:
                print(f"            🔍 Test verso {neighbor_label} VIA {rel_type}")

                df_n = build_neighbor_df(
                    session, target_label, neighbor_label, rel_type
                )

                if df_n.shape[0] < 2 or df_n.shape[1] < 2:
                    print("               ⏭️ Dataset vicino troppo piccolo")
                    continue

                print("               🔍 Calcolo chiavi esterne...")
                neighbor_keys = compute_keys(df_n)

                current_memory = process.memory_info().rss / (1024 ** 2)
                max_memory = max(max_memory, current_memory)

                if not neighbor_keys:
                    print("               ❌ Nessuna chiave")
                    continue

                if not check_coverage(session, target_label, neighbor_label, rel_type):
                    print("               ❌ Coverage fallita")
                    continue

                if not check_one_to_one(session, target_label, neighbor_label, rel_type):
                    print("               ❌ One-to-one fallita")
                    continue


                print(f"               ✅ EXTERNAL_KEY trovata: {neighbor_keys}")

                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(
                        f"{sub} - EXTERNAL_KEY "
                        f"{neighbor_label}{neighbor_keys} "
                        f"VIA {rel_type}\n"
                    )
            external_time += (time.time() - t0)
    # --------------------------------------------------------
    # 📊 RIEPILOGO FINALE
    # --------------------------------------------------------
    end_time = time.time()
    end_memory = process.memory_info().rss / (1024 ** 2)
    elapsed_time = end_time - start_time

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n")
        f.write(f"tempo d'esecuzione totale: {elapsed_time:.3f} s\n")
        f.write(f"internal keys discovery time: {internal_time:.3f} s\n")
        f.write(f"external keys discovery time: {external_time:.3f} s\n")
        #f.write(f"memoria occupata: {end_memory:.2f} MB\n")
        f.write(f"memoria massima totale: {max_memory:.2f} MB\n")

    print(f"\n📄 Log salvato in: {LOG_FILE}")
    print(f"⏱️ Tempo chiavi interne: {internal_time:.2f} s")
    print(f"⏱️ Tempo chiavi esterne: {external_time:.2f} s")
    print(f"⏱️ Tempo totale: {elapsed_time:.2f} s")
    print(f"🧠 Memoria max: {max_memory:.2f} MB")


# ------------------------------------------------------------
# ✅ Example usage
# ------------------------------------------------------------
nomedb = "synthea_1000"
numpattern = 8
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
    process_pattern(pattern, nomedb, numpattern, lenpattern)
    numpattern += 1
