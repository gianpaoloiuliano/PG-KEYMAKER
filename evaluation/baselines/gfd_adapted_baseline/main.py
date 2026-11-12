import sys
from neo4j_connector import Neo4jConnector
from gfd_adapter import run_discovery, render_report

# ==========================
# CONFIGURAZIONE
# ==========================

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "gianpaolo"
typology="meaningfull"

dataset="synthea_900"

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


MIN_SUPPORT = 1
TRACK_MEMORY = True

def main():
    num_query = 101
    for q in patterns:
        q, filters_str = q if isinstance(q, tuple) else (q, "")
        try:
            connector = Neo4jConnector(URI, USER, PASSWORD)
        except Exception as e:
            print(f"[errore] connessione a Neo4j fallita: {e}", file=sys.stderr)
            sys.exit(1)

        try:
            results = run_discovery(
                connector,
                scope_str=q,
                min_support=MIN_SUPPORT,
                track_memory=TRACK_MEMORY,
                filters_str=filters_str,
            )
        except Exception as e:
            print(f"[errore] durante la scoperta: {e}", file=sys.stderr)
            connector.close()
            sys.exit(1)
        finally:
            connector.close()

        report = render_report(results, MIN_SUPPORT)

        print(report)

        OUTPUT = f"../../scalability/baseline_gfd/{typology}/results_gfd_{dataset}_{num_query}.txt"  # oppure None se non vuoi salvare

        if OUTPUT:
            try:
                with open(OUTPUT, "w", encoding="utf-8") as f:
                    if filters_str:
                        f.write(f"filters: {filters_str}\n")
                    f.write(report + "\n")
                print(f"\n[info] Report salvato in {OUTPUT}")
            except OSError as e:
                print(f"[errore] impossibile scrivere {OUTPUT}: {e}", file=sys.stderr)

        num_query = num_query + 1

if __name__ == "__main__":
    main()