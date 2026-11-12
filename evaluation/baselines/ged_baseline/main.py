import os
import sys
from ged_adapter import Neo4jConnector, run_discovery, render_report

# ==========================
# CONFIGURATION
# ==========================

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "gianpaolo"
typology="meaningfull"

dataset="synthea_900"
num_query = 101

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


TRACK_MEMORY = True

def main():
    global num_query
    for q in patterns:
        q, filters_str = q if isinstance(q, tuple) else (q, "")
        try:
            connector = Neo4jConnector(URI, USER, PASSWORD)
        except Exception as e:
            print(f"[error] Neo4j connection failed: {e}", file=sys.stderr)
            sys.exit(1)

        try:
            results = run_discovery(
                connector,
                scope_str=q,
                track_memory=TRACK_MEMORY,
                filters_str=filters_str,
            )
        except Exception as e:
            print(f"[error] during discovery: {e}", file=sys.stderr)
            connector.close()
            sys.exit(1)
        finally:
            connector.close()

        report = render_report(results)

        print(report)

        OUTPUT = f"../../scalability/baseline_ged/{typology}/results_ged_{dataset}_{num_query}.txt"  # or None if you do not want to save

        if OUTPUT:
            try:
                os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
                with open(OUTPUT, "w", encoding="utf-8") as f:
                    if filters_str:
                        f.write(f"filters: {filters_str}\n")
                    f.write(report + "\n")
                print(f"\n[info] Report saved to {OUTPUT}")
            except OSError as e:
                print(f"[error] unable to write {OUTPUT}: {e}", file=sys.stderr)

        num_query = num_query + 1

if __name__ == "__main__":
    main()
