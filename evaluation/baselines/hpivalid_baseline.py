from neo4j import GraphDatabase
import pandas as pd
import os, sys, re, time, psutil
import subprocess

# query_filters lives in the project root (two levels above this folder)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import query_filters as qf

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


# Fixed CSV name, as in metanome_ucc.py: it is overwritten on every call
TMP_CSV = "tmp.csv"


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
    return qf.baseline_subpatterns(pattern)

def generate_query_from_pattern(pattern_str, filters=None):
    pattern_str = pattern_str.strip()

    if pattern_str.startswith("(") and pattern_str.endswith(")"):
        return f"{qf.build_match_block(pattern_str, filters)} RETURN {qf.return_star(pattern_str, filters)}"

    if pattern_str.startswith("[") and pattern_str.endswith("]"):
        return f"{qf.build_match_block(f'()-{pattern_str}-()', filters)} RETURN {pattern_str[1:pattern_str.find(':')]}"

    return f"{qf.build_match_block(pattern_str, filters)} RETURN {qf.return_star(pattern_str, filters)}"

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
def process_pattern(pattern, nomedb, numpattern, lenpattern, filters_str=""):

    print(f"\n🚀 PATTERN #{numpattern}")
    print(f"   {pattern}")
    if filters_str:
        qf.dbg(f"filters: {filters_str}")
    filters = qf.parse_filters(filters_str, pattern)

    start_time = time.time()
    process = psutil.Process(os.getpid())

    internal_time = 0
    external_time = 0

    start_memory = process.memory_info().rss / (1024 ** 2)
    max_memory = start_memory

    LOG_DIR = f"../scalability/baseline_hpivalid/{typology}"
    LOG_FILE = os.path.join(
        LOG_DIR,
        f"results_HPIValid_{nomedb}_{numpattern}.txt"
    )
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n### PATTERN {pattern}\n")
        if filters_str:
            f.write(f"### FILTERS {filters_str}\n")

    subpatterns = extract_all_valid_subpatterns(pattern)

    with driver.session() as session:

        for i, sub in enumerate(subpatterns, start=1):

            t0 = time.time()
            sub_filters = qf.filters_for_subpattern(filters, sub)
            print(f"   ▶ Subpattern {i}/{len(subpatterns)}: {sub}{qf.filters_label(sub_filters)}")

            query = generate_query_from_pattern(sub, sub_filters)
            df = session.execute_read(run_query, query)

            if df.shape[0] < 2 or df.shape[1] < 1:
                print("      ⏭️ too small")
                continue

            # ---------------- INTERNAL KEYS ----------------
            print("      🔍 internal HPIValid...")

            csv_name = TMP_CSV
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

                if df_n.shape[0] < 2 or df_n.shape[1] < 1:
                    continue

                csv_name = TMP_CSV
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
        f.write(f"total execution time: {elapsed:.3f} s\n")
        f.write(f"internal keys discovery time: {internal_time:.3f} s\n")
        f.write(f"external keys discovery time: {external_time:.3f} s\n")
        f.write(f"max total memory: {max_memory:.2f} MB\n")

    print(f"\n📄 Log saved to: {LOG_FILE}")
    print(f"⏱️ Internal keys time: {internal_time:.2f} s")
    print(f"⏱️ External keys time: {external_time:.2f} s")
    print(f"⏱️ Total time: {elapsed:.2f} s")
    print(f"🧠 Max memory: {max_memory:.2f} MB")

# ------------------------------------------------------------
# ✅ Run
# ------------------------------------------------------------
nomedb = "synthea_900"
numpattern = 101
lenpattern = "len_2"
typology = "meaningfull"


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
# ===================== HETIONET =====================
patterns = [
    ("(g:Gene)-[gig1:INTERACTS_GiG]->(g2:Gene)-[gig2:INTERACTS_GiG]->(g3:Gene)",""),
    ("(a:Anatomy)-[ex:EXPRESSES_AeG]->(g:Gene)-[par:PARTICIPATES_GpBP]->(bp:BiologicalProcess)",""),
    ("(g:Gene)-[reg:REGULATES_GrG]->(g2:Gene)-[reg2:REGULATES_GrG]->(g3:Gene)",""),
    ("(g:Gene)-[reg:REGULATES_GrG]->(g2:Gene)-[part:PARTICIPATES_GpBP]->(bp:BiologicalProcess)",""),
    ("(a:Anatomy)-[ex:EXPRESSES_AeG]->(g:Gene)-[reg:REGULATES_GrG]->(g2:Gene)",""),
    ("(g:Gene)-[gig1:INTERACTS_GiG]->(g2:Gene)-[par:PARTICIPATES_GpBP]->(bp:BiologicalProcess)",""),
    ("(a:Anatomy)-[exp:EXPRESSES_AeG]->(g2:Gene)-[gig1:INTERACTS_GiG]->(g3:Gene)",""),
    ("(dis:Disease)-[loc:LOCALIZES_DlA]->(a:Anatomy)-[exp:EXPRESSES_AeG]->(g:Gene)","")
]
patterns = [
    # Polypharmacology: compounds that bind many targets with high affinity, and the associated diseases
    ("(c:Compound)-[b:BINDS_CbG]-(g:Gene)-[a:ASSOCIATES_DaG]-(d:Disease)",
     "b.affinity_nM < 100 AND count(g) > 5 PER c"),

    # Signature reversal: the drug strongly downregulates a gene that the disease
    # upregulates at least two-fold; diseases with many candidate drugs
    ("(c:Compound)-[cd:DOWNREGULATES_CdG]-(g:Gene)-[du:UPREGULATES_DuG]-(d:Disease)",
     "abs(cd.z_score) >= 5 AND du.log2_fold_change >= 1 AND count(c) > 5 PER d"),

    # Repositioning: diseases with several treatments for which structurally very similar compounds exist
    ("(c1:Compound)-[r:RESEMBLES_CrC]-(c2:Compound)-[t:TREATS_CtD]-(d:Disease)",
     "r.similarity >= 0.5 AND count(c2) > 2 PER d"),
     
     # Complex syndromes: diseases treated with a broad symptom profile
    ("(c:Compound)-[t:TREATS_CtD]-(d:Disease)-[p:PRESENTS_DpS]-(s:Symptom)",
     "count(s) > 10 PER d"),

    # Genetic comorbidity: diseases sharing GWAS-supported genes with many other diseases
    ("(d1:Disease)-[a1:ASSOCIATES_DaG]-(g:Gene)-[a2:ASSOCIATES_DaG]-(d2:Disease)",
     "a1.unbiased = true AND a2.unbiased = true AND count(d2) > 3 PER d1"),


    # Class effects: pharmacologic classes by mechanism of action with many compounds, and their side effects
    ("(pc:PharmacologicClass)-[i:INCLUDES_PCiC]-(c:Compound)-[cs:CAUSES_CcSE]-(se:SideEffect)",
     "pc.class_type = 'Mechanism of Action' AND count(c) > 5 PER pc"),
]

#network
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


#bloom
patterns = [
    "(ah:AccountHolder)-[has:HAS_BANKACCOUNT]-(ba:BankAccount)-[f:FROM]-(fi:FinancialInstitute)",
    "(mt:MoneyTransfer)-[s:SEND]-(ba:BankAccount)-[fr:FROM]-(fi:FinancialInstitute)",
    "(ba:BankAccount)-[s:SEND]-(mt:MoneyTransfer)-[s1:SEND]-(ba2:BankAccount)",
    "(ba:BankAccount)-[s:SEND]-(mt:MoneyTransfer)-[w:WITH]-(pn:PhoneNumber)",
    "(ah:AccountHolder)-[has:HAS_BANKACCOUNT]-(ba:BankAccount)-[s:SEND]-(mt:MoneyTransfer)",
    "(pu:Purchase)-[fromip:FROM_IP]-(ip:IP)-[li:LOCATED_IN]-(st:State)",
    "(pu:Purchase)-[wl:WITH_LOGIN]-(lo:Login)-[fs:FOR_SHOP]-(sh:Shop)",
    "(ah:AccountHolder)-[hpn:HAS_PHONENUMBER]-(pn:PhoneNumber)-[hpn2:HAS_PHONENUMBER]-(ul:UnsecuredLoan)",
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
    pattern, filters_str = pattern if isinstance(pattern, tuple) else (pattern, "")
    process_pattern(pattern, nomedb, numpattern, lenpattern, filters_str)
    numpattern += 1


