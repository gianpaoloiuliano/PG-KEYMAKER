"""
Lancia in sequenza gli esperimenti synthea (hyucc e hpivalid, con e senza albero, 3/4/5 hop).
Per ogni synthea: prima tutti i test senza albero, poi tutti quelli con albero.
Da PyCharm: tasto destro sul file -> Run 'run_synthea_experiments'.
"""
import os
import subprocess
import sys

UCC_ALGS = ["hyucc", "hpivalid"]
USE_DB_SWITCH = False       # True = passa --database <synthea_N> (solo se i db stanno nella stessa istanza Neo4j)
SKIP_EXISTING = True        # True = resume: salta i test con file risultato già completo; False = rifà tutto
PAUSE_BETWEEN_DBS = True   # True = prima di ogni synthea_N aspetta che avvii l'istanza in Neo4j Desktop
DBS = [f"synthea_{n}" for n in range(600, 1000, 100)]   # 100..900 (il 1000 è già fatto)
HOPS = [(3, 31), (4, 41), (5, 51)]                      # (hop, numtest iniziale)

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
    """True se la combinazione ha ancora test da fare (controllo sui file, senza toccare Neo4j)."""
    if not SKIP_EXISTING:
        return True
    return subprocess.run(build_cmd(db, tree, alg, hop, start) + ["--check-only"],
                          cwd=HERE, stdout=subprocess.DEVNULL).returncode != 0


def run(db, tree, alg, hop, start):
    print(f"\n=== {db} | tree={tree} | {alg} | {hop} hop | numtest={start} ===", flush=True)
    result = subprocess.run(build_cmd(db, tree, alg, hop, start), cwd=HERE)
    if result.returncode != 0:
        print(f"FALLITO: {db} tree={tree} {alg} {hop} hop (exit {result.returncode}), proseguo", flush=True)


def main():
    for db in DBS:
        combos = [(tree, alg, hop, start)
                  for tree in (False, True) for alg in UCC_ALGS for hop, start in HOPS]
        pending = [c for c in combos if is_pending(db, *c)]
        if not pending:
            print(f"{db}: tutto già fatto, salto", flush=True)
            continue
        if PAUSE_BETWEEN_DBS:
            input(f"Avvia l'istanza {db} in Neo4j Desktop (fermando quella precedente) e premi Invio ")
        for combo in pending:
            run(db, *combo)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrotto. Rilancia lo script per riprendere da dove eri rimasto.", flush=True)
