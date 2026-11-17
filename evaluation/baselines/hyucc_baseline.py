from neo4j import GraphDatabase
import pandas as pd
import openclean_metanome.config as config
from openclean_metanome.algorithm.hyucc import hyucc
import os, sys, re, time, psutil

# query_filters lives in the project root (two levels above this folder)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import query_filters as qf

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
    # HyUCC fails (ArrayIndexOutOfBounds in the Sampler) on a single column with
    # duplicates: with a single column, a key exists if and only if the values are unique
    if df.shape[1] == 1:
        col = df.columns[0]
        return [[col]] if df[col].nunique(dropna=False) == len(df) else []
    env ={config.METANOME_JARPATH: config.JARFILE()}
    return hyucc(df, env=env, verbose=False, max_ucc_size=-1)


# ------------------------------------------------------------
# ✅ Pattern utilities
# ------------------------------------------------------------
def extract_all_valid_subpatterns(pattern):
    return qf.baseline_subpatterns(pattern)


def generate_query_from_pattern(pattern_str, filters=None):
    pattern_str = pattern_str.strip()

    # node only
    if pattern_str.startswith("(") and pattern_str.endswith(")"):
        return f"{qf.build_match_block(pattern_str, filters)} RETURN {qf.return_star(pattern_str, filters)}"

    # relationship only
    if pattern_str.startswith("[") and pattern_str.endswith("]"):
        return f"{qf.build_match_block(f'()-{pattern_str}-()', filters)} RETURN {pattern_str[1:pattern_str.find(':')]}"

    # mixed pattern
    return f"{qf.build_match_block(pattern_str, filters)} RETURN {qf.return_star(pattern_str, filters)}"



# ------------------------------------------------------------
# ✅ Neo4j connection
# ------------------------------------------------------------
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))


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
# ✅ Main processing (WITH DEBUG)
# ------------------------------------------------------------
def process_pattern(pattern, nomedb, numpattern, lenpattern, filters_str=""):

    print(f"\n🚀 Starting PATTERN #{numpattern}")
    print(f"   Pattern: {pattern}")
    if filters_str:
        qf.dbg(f"filters: {filters_str}")
    filters = qf.parse_filters(filters_str, pattern)

    start_time = time.time()
    process = psutil.Process(os.getpid())
    # separate timers
    internal_time = 0
    external_time = 0

    start_memory = process.memory_info().rss / (1024 ** 2)
    max_memory = start_memory

    LOG_DIR = f"../scalability/baseline_hyucc/{typology}"
    LOG_FILE = os.path.join(
        LOG_DIR,
        f"results_HyUCC_bs1_{nomedb}_{numpattern}.txt"
    )
    os.makedirs(LOG_DIR, exist_ok=True)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n### PATTERN {pattern}\n")
        if filters_str:
            f.write(f"### FILTERS {filters_str}\n")

    subpatterns = extract_all_valid_subpatterns(pattern)
    print(f"   🔹 Subpatterns found: {len(subpatterns)}")

    with driver.session() as session:
        for i, sub in enumerate(subpatterns, start=1):
            t0 = time.time()
            sub_filters = qf.filters_for_subpattern(filters, sub)
            print(f"      ▶ Subpattern {i}/{len(subpatterns)}: {sub}{qf.filters_label(sub_filters)}")

            query = generate_query_from_pattern(sub, sub_filters)
            df = session.execute_read(run_query, query)

            if df.shape[0] < 2 or df.shape[1] < 1:
                print("         ⏭️ Dataset too small")
                continue

            # ---- INTERNAL KEYS ----
            print("         🔍 Computing internal keys...")


            keys = compute_keys(df)
            t1 = time.time()

            internal_time += (t1 - t0)

            current_memory = process.memory_info().rss / (1024 ** 2)
            max_memory = max(max_memory, current_memory)

            if keys:
                print(f"         ✅ INTERNAL_KEYS found: {keys}")
                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(f"{sub} - INTERNAL_KEYS {keys}\n")
                continue

            # ---- BASELINE 1 / EXTERNAL KEYS ----
            if not is_single_node_pattern(sub):
                print("         ⏭️ Not a single-node pattern")
                continue

            t0 = time.time()
            target_label = extract_node_label(sub)
            neighbors = find_neighbors(session, target_label)

            print(f"         🔗 Neighbors found: {len(neighbors)}")

            for neighbor_label, rel_type in neighbors:
                print(f"            🔍 Testing towards {neighbor_label} VIA {rel_type}")

                df_n = build_neighbor_df(
                    session, target_label, neighbor_label, rel_type
                )

                if df_n.shape[0] < 2 or df_n.shape[1] < 1:
                    print("               ⏭️ Neighbor dataset too small")
                    continue

                print("               🔍 Computing external keys...")
                neighbor_keys = compute_keys(df_n)

                current_memory = process.memory_info().rss / (1024 ** 2)
                max_memory = max(max_memory, current_memory)

                if not neighbor_keys:
                    print("               ❌ No key")
                    continue

                if not check_coverage(session, target_label, neighbor_label, rel_type):
                    print("               ❌ Coverage failed")
                    continue

                if not check_one_to_one(session, target_label, neighbor_label, rel_type):
                    print("               ❌ One-to-one failed")
                    continue


                print(f"               ✅ EXTERNAL_KEY found: {neighbor_keys}")

                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(
                        f"{sub} - EXTERNAL_KEY "
                        f"{neighbor_label}{neighbor_keys} "
                        f"VIA {rel_type}\n"
                    )
            external_time += (time.time() - t0)
    # --------------------------------------------------------
    # 📊 FINAL SUMMARY
    # --------------------------------------------------------
    end_time = time.time()
    end_memory = process.memory_info().rss / (1024 ** 2)
    elapsed_time = end_time - start_time

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n")
        f.write(f"total execution time: {elapsed_time:.3f} s\n")
        f.write(f"internal keys discovery time: {internal_time:.3f} s\n")
        f.write(f"external keys discovery time: {external_time:.3f} s\n")
        #f.write(f"memory used: {end_memory:.2f} MB\n")
        f.write(f"max total memory: {max_memory:.2f} MB\n")

    print(f"\n📄 Log saved to: {LOG_FILE}")
    print(f"⏱️ Internal keys time: {internal_time:.2f} s")
    print(f"⏱️ External keys time: {external_time:.2f} s")
    print(f"⏱️ Total time: {elapsed_time:.2f} s")
    print(f"🧠 Max memory: {max_memory:.2f} MB")


# ------------------------------------------------------------
# ✅ Example usage
# ------------------------------------------------------------

nomedb = "synthea_200"
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
