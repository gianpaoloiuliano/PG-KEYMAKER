"""
Run the synthea experiments in sequence (hyucc and hpivalid, with and without tree, 3/4/5 hops).
For each synthea: first all the tests without tree, then all those with tree.
From PyCharm: right-click the file -> Run 'run_synthea_experiments'.
"""
import os
import subprocess
import sys

UCC_ALGS = ["hyucc", "hpivalid"]
USE_DB_SWITCH = False       # True = pass --database <synthea_N> (only if the dbs live in the same Neo4j instance)
SKIP_EXISTING = True        # True = resume: skip tests whose result file is already complete; False = redo everything
PAUSE_BETWEEN_DBS = True   # True = before each synthea_N, wait for you to start the instance in Neo4j Desktop
DBS = [f"synthea_{n}" for n in range(100, 1000, 100)]   # 100..900 (1000 is already done)
HOPS = [(3, 31), (4, 41), (5, 51)]                      # (hop, initial numtest)

HERE = os.path.dirname(os.path.abspath(__file__))


def build_cmd(db, tree, alg, hop, start):
    cmd = [
        sys.executable, "-u", "testing_optimized_wargs.py",
        "--db", db,
        "--numtest", str(start),
        "--tree", str(tree).lower(),
        "--ucc-alg", alg,
        "--patterns-file", f"patterns/synthea_{hop}hops.json",
        "--skip-existing", str(SKIP_EXISTING).lower(),
    ]
    if USE_DB_SWITCH:
        cmd += ["--database", db]
    return cmd


def is_pending(db, tree, alg, hop, start):
    """True if the combination still has tests to run (checks files only, without touching Neo4j)."""
    if not SKIP_EXISTING:
        return True
    return subprocess.run(build_cmd(db, tree, alg, hop, start) + ["--check-only"],
                          cwd=HERE, stdout=subprocess.DEVNULL).returncode != 0


def run(db, tree, alg, hop, start):
    print(f"\n=== {db} | tree={tree} | {alg} | {hop} hop | numtest={start} ===", flush=True)
    result = subprocess.run(build_cmd(db, tree, alg, hop, start), cwd=HERE)
    if result.returncode != 0:
        print(f"FAILED: {db} tree={tree} {alg} {hop} hop (exit {result.returncode}), continuing", flush=True)


def main():
    for db in DBS:
        combos = [(tree, alg, hop, start)
                  for tree in (False, True) for alg in UCC_ALGS for hop, start in HOPS]
        pending = [c for c in combos if is_pending(db, *c)]
        if not pending:
            print(f"{db}: everything already done, skipping", flush=True)
            continue
        if PAUSE_BETWEEN_DBS:
            input(f"Start the instance {db} in Neo4j Desktop (stopping the previous one) and press Enter ")
        for combo in pending:
            run(db, *combo)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted. Re-run the script to resume where you left off.", flush=True)
