from neo4j import GraphDatabase
import pandas as pd
import os, sys, re, time, psutil
import subprocess

# ------------------------------------------------------------
# ✅ PATH CONFIG
# ------------------------------------------------------------
PROJECT_DIR = "/mnt/c/Users/gianp/Desktop/HPIvalid/release"
CSV_DIR = "/mnt/c/Users/gianp/Desktop/temp_csvs"

os.makedirs(CSV_DIR, exist_ok=True)

# ------------------------------------------------------------
# ✅ CSV SAVE
# ------------------------------------------------------------
def save_csv(df, name):
    path = os.path.join("C:/Users\gianp\Desktop/temp_csvs", name)
    df.to_csv(path, index=False)
    return path


# ------------------------------------------------------------
# ✅ PATTERN → FILENAME
# ------------------------------------------------------------
def pattern_to_filename(pattern_str):
    return (
        pattern_str
        .replace("(", "")
        .replace(")", "")
        .replace("[", "")
        .replace("]", "")
        .replace(":", "_")
        .replace("-", "_")
        .replace(" ", "")
    )


# ------------------------------------------------------------
# ✅ HPIValid UCC discovery
# ------------------------------------------------------------
def compute_keys(csv_name):

    cmd = [
        "wsl",
        "-d",
        "Ubuntu",
        "sh",
        "-c",
        f"cd '{PROJECT_DIR}' && ./HPIValid -h -i '{CSV_DIR}/{csv_name}' -o '{CSV_DIR}'"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    output = result.stdout

    ucc = 0
    for line in output.splitlines():
        if "minimal UCCs:" in line:
            ucc = int(line.split(":")[1].strip())
            break

    return ucc


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

    if pattern_str.startswith("(") and pattern_str.endswith(")"):
        return f"MATCH {pattern_str} RETURN *"

    if pattern_str.startswith("[") and pattern_str.endswith("]"):
        return f"MATCH ()-{pattern_str}-() RETURN {pattern_str[1:pattern_str.find(':')]}"

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
# ✅ Utilities
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
# ✅ MAIN PROCESS
# ------------------------------------------------------------
def process_pattern(pattern, nomedb, numpattern, lenpattern):

    print(f"\n🚀 PATTERN #{numpattern}")
    print(f"   {pattern}")

    start_time = time.time()
    process = psutil.Process(os.getpid())

    internal_time = 0
    external_time = 0

    start_memory = process.memory_info().rss / (1024 ** 2)
    max_memory = start_memory

    LOG_DIR = "../scalability/baseline_hpivalid"
    LOG_FILE = os.path.join(
        LOG_DIR,
        f"results_HPIValid_{nomedb}_{numpattern}.txt"
    )
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n### PATTERN {pattern}\n")

    subpatterns = extract_all_valid_subpatterns(pattern)

    with driver.session() as session:

        for i, sub in enumerate(subpatterns, start=1):

            t0 = time.time()
            print(f"   ▶ Subpattern {i}/{len(subpatterns)}: {sub}")

            query = generate_query_from_pattern(sub)
            df = session.execute_read(run_query, query)

            if df.shape[0] < 2 or df.shape[1] < 2:
                print("      ⏭️ troppo piccolo")
                continue

            # ---------------- INTERNAL KEYS ----------------
            print("      🔍 internal HPIValid...")

            csv_name = f"internal_{pattern_to_filename(sub)}.csv"
            csv_path = save_csv(df, csv_name)
            ucc_count = compute_keys(csv_name)

            internal_time += (time.time() - t0)
            current_memory = process.memory_info().rss / (1024 ** 2)
            max_memory = max(max_memory, current_memory)

            if ucc_count > 0:
                print(f"      ✅ INTERNAL KEYS: {ucc_count}")
                continue

            # ---------------- EXTERNAL KEYS ----------------
            if not is_single_node_pattern(sub):
                continue

            t0 = time.time()

            target_label = extract_node_label(sub)
            neighbors = find_neighbors(session, target_label)

            for j, (neighbor_label, rel_type) in enumerate(neighbors):

                print(f"         🔍 {neighbor_label} via {rel_type}")

                df_n = build_neighbor_df(session, target_label, neighbor_label, rel_type)

                if df_n.shape[0] < 2 or df_n.shape[1] < 2:
                    continue

                csv_name = f"external_{pattern_to_filename(sub)}__{neighbor_label}_{rel_type}.csv"
                csv_path = save_csv(df_n, csv_name)

                neighbor_ucc = compute_keys(csv_name)

                if neighbor_ucc == 0:
                    continue

                if not check_coverage(session, target_label, neighbor_label, rel_type):
                    continue

                if not check_one_to_one(session, target_label, neighbor_label, rel_type):
                    continue

                print(f"      ✅ EXTERNAL KEY: {neighbor_label}[{neighbor_ucc}]")
                current_memory = process.memory_info().rss / (1024 ** 2)
                max_memory = max(max_memory, current_memory)
                #with open(LOG_FILE, "a", encoding="utf-8") as f:
                #    f.write(f"{sub} - EXTERNAL_KEY {neighbor_label}[{neighbor_ucc}] VIA {rel_type}\n")

            external_time += (time.time() - t0)

    elapsed = time.time() - start_time
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n")
        f.write(f"tempo d'esecuzione totale: {elapsed:.3f} s\n")
        f.write(f"internal keys discovery time: {internal_time:.3f} s\n")
        f.write(f"external keys discovery time: {external_time:.3f} s\n")
        f.write(f"memoria massima totale: {max_memory:.2f} MB\n")

    print(f"\n📄 Log salvato in: {LOG_FILE}")
    print(f"⏱️ Tempo chiavi interne: {internal_time:.2f} s")
    print(f"⏱️ Tempo chiavi esterne: {external_time:.2f} s")
    print(f"⏱️ Tempo totale: {elapsed:.2f} s")
    print(f"🧠 Memoria max: {max_memory:.2f} MB")

# ------------------------------------------------------------
# ✅ Run
# ------------------------------------------------------------
nomedb = "finbench"
numpattern = 1
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


