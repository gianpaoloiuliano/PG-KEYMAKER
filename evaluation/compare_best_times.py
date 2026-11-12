"""
Confronta i tempi totali (in secondi) di tutte le versioni di PG-KEYMAKER e delle baseline,
leggendo direttamente i file results_*.txt in evaluation/scalability, e indica per ogni
query (scenario, dataset, query) la versione con il tempo migliore.

Uso:
    python compare_best_times.py                      # solo stacked/w_filters/other_shapes, senza ablation
    python compare_best_times.py --include-ablation   # include anche le varianti NOTREE
    python compare_best_times.py --scenario stacked --dataset finbench
    python compare_best_times.py --csv best_times.csv # salva anche la tabella completa
"""
import argparse
import os
import re
import sys

import pandas as pd

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scalability")

FILE_RE = re.compile(
    r"^results_.+?_(synthea_\d+(?:_alt)?|finbench|network-management|fraud|hetionet)_(\d+)\.txt$"
)

MAX_QUERY = 16
EXTRA_QUERIES = {21, 22, 31, 32, 51, 52, 53}  # other_shapes (2x, 3x) e meaningfull (5x)
NUM = r"([\d\.]+)\s*s"
PG_PARTS = [
    r"execution time for tree and leaves:\s*" + NUM,
    r"execution time for external keys:\s*" + NUM,
    r"execution time for case 1:\s*" + NUM,
    r"execution time for case 2:\s*" + NUM,
    r"execution time for case 3:\s*" + NUM,
]
BASE_INT = re.compile(r"internal keys discovery time:\s*" + NUM, re.I)
BASE_EXT = re.compile(r"external keys discovery time:\s*" + NUM, re.I)
TOTAL_LINE = re.compile(r"(?:tempo d'esecuzione totale::?|total execution time:)\s*" + NUM, re.I)


def total_time(content):
    """Tempo totale in secondi (stessa logica di results_and_plots_v2.py), None se non trovato."""
    pg = [re.search(p, content, re.I) for p in PG_PARTS]
    if any(pg):  # output di PG-KEYMAKER: somma delle fasi
        return round(sum(float(m.group(1)) for m in pg if m), 2)
    t = TOTAL_LINE.search(content)  # baseline (HyUCC, HPIValid, GFD): riga del tempo totale
    if t:
        return float(t.group(1))
    i, e = BASE_INT.search(content), BASE_EXT.search(content)
    if i or e:  # fallback se manca la riga totale: interno + esterno
        return round((float(i.group(1)) if i else 0.0) + (float(e.group(1)) if e else 0.0), 3)
    return None


def collect(include_ablation):
    records = []
    for dirpath, _, files in os.walk(ROOT):
        rel = os.path.relpath(dirpath, ROOT).split(os.sep)
        if len(rel) < 2:
            continue
        if rel[0] == "ablation" and not include_ablation:
            continue
        # versione = cartella dell'algoritmo; scenario = sottocartella (stacked, w_filters, ...)
        version, scenario = (rel[1], rel[2]) if rel[0] == "ablation" and len(rel) > 2 else (rel[0], rel[1])
        if rel[0] == "ablation" and len(rel) == 2:
            version, scenario = rel[1], "stacked"
        for f in files:
            m = FILE_RE.match(f)
            q = int(m.group(2)) if m else None
            if not m or (q > MAX_QUERY and q not in EXTRA_QUERIES):  # scarta file di test (es. query 7777777)
                continue
            with open(os.path.join(dirpath, f), encoding="utf-8", errors="replace") as fh:
                t = total_time(fh.read())
            if not t:  # None o 0.0: run fallita/incompleta
                print(f"[warn] nessun tempo valido in {os.path.join(dirpath, f)}", file=sys.stderr)
                continue
            ds = m.group(1).replace("_alt", "")
            records.append(dict(scenario=scenario, dataset=ds, query=int(m.group(2)),
                                version=version, time=t))
    return pd.DataFrame(records)


def ds_key(ds):
    return (0, int(ds.split("_")[1])) if ds.startswith("synthea_") else (1, ds)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--include-ablation", action="store_true")
    ap.add_argument("--scenario", help="filtra per scenario (stacked, w_filters, other_shapes)")
    ap.add_argument("--dataset", help="filtra per dataset (es. finbench, synthea_100, hetionet)")
    ap.add_argument("--csv", help="salva la tabella completa (una riga per query, una colonna per versione)")
    args = ap.parse_args()

    df = collect(args.include_ablation)
    if df.empty:
        sys.exit("Nessun risultato trovato.")
    if args.scenario:
        df = df[df.scenario == args.scenario]
    if args.dataset:
        df = df[df.dataset == args.dataset]

    dup = df.duplicated(["scenario", "dataset", "query", "version"], keep=False)
    if dup.any():
        print(f"[warn] {dup.sum()} file duplicati per (scenario,dataset,query,versione): tengo il tempo minimo\n",
              file=sys.stderr)
    df = df.groupby(["scenario", "dataset", "query", "version"], as_index=False)["time"].min()

    wide = df.pivot(index=["scenario", "dataset", "query"], columns="version", values="time").reset_index()
    versions = [c for c in wide.columns if c not in ("scenario", "dataset", "query")]
    wide["best_version"] = wide[versions].idxmin(axis=1)
    wide["best_time"] = wide[versions].min(axis=1)
    # seconda migliore, per vedere il margine
    def second_best(r):
        s = r.dropna().sort_values(kind="stable")
        return (s.index[1], s.iloc[1]) if len(s) > 1 else (None, float("nan"))

    sb = wide[versions].apply(second_best, axis=1)
    wide["second_version"] = sb.map(lambda x: x[0])
    wide["second_time"] = sb.map(lambda x: x[1])
    wide["speedup_vs_2nd"] = wide["second_time"] / wide["best_time"]
    wide = wide.sort_values(["scenario", "dataset", "query"],
                            key=lambda s: s.map(ds_key) if s.name == "dataset" else s).reset_index(drop=True)

    pd.set_option("display.width", 250)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)

    print("=== Tempo migliore per ogni query (secondi) ===")
    out = wide[["scenario", "dataset", "query", "best_version", "best_time",
                "second_version", "second_time", "speedup_vs_2nd"]].copy()
    out["speedup_vs_2nd"] = out["speedup_vs_2nd"].round(2)
    print(out.to_string(index=False))

    print("\n=== Numero di query vinte per versione ===")
    wins = wide.groupby(["scenario", "best_version"]).size().rename("wins").reset_index()
    tot = wide.groupby("scenario").size().rename("queries")
    wins = wins.join(tot, on="scenario")
    print(wins.sort_values(["scenario", "wins"], ascending=[True, False]).to_string(index=False))

    if args.csv:
        wide.to_csv(args.csv, sep=";", index=False)
        print(f"\nTabella completa salvata in {args.csv}")


if __name__ == "__main__":
    main()
