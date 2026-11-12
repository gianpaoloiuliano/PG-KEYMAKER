"""
Compares the total times (in seconds) of all PG-KEYMAKER versions and baselines,
reading the results_*.txt files directly in evaluation/scalability, and indicates for each
query (scenario, dataset, query) the version with the best time.

Usage:
    python compare_best_times.py                      # only stacked/w_filters/other_shapes, without ablation
    python compare_best_times.py --include-ablation   # also include the NOTREE variants
    python compare_best_times.py --scenario stacked --dataset finbench
    python compare_best_times.py --csv best_times.csv # also save the full table
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
EXTRA_QUERIES = {21, 22, 31, 32, 51, 52, 53}  # other_shapes (2x, 3x) and meaningfull (5x)
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
    """Total time in seconds (same logic as results_and_plots_v2.py), None if not found."""
    pg = [re.search(p, content, re.I) for p in PG_PARTS]
    if any(pg):  # PG-KEYMAKER output: sum of the phases
        return round(sum(float(m.group(1)) for m in pg if m), 2)
    t = TOTAL_LINE.search(content)  # baseline (HyUCC, HPIValid, GFD): total time line
    if t:
        return float(t.group(1))
    i, e = BASE_INT.search(content), BASE_EXT.search(content)
    if i or e:  # fallback if the total line is missing: internal + external
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
        # version = algorithm folder; scenario = subfolder (stacked, w_filters, ...)
        version, scenario = (rel[1], rel[2]) if rel[0] == "ablation" and len(rel) > 2 else (rel[0], rel[1])
        if rel[0] == "ablation" and len(rel) == 2:
            version, scenario = rel[1], "stacked"
        for f in files:
            m = FILE_RE.match(f)
            q = int(m.group(2)) if m else None
            if not m or (q > MAX_QUERY and q not in EXTRA_QUERIES):  # discard test files (e.g. query 7777777)
                continue
            with open(os.path.join(dirpath, f), encoding="utf-8", errors="replace") as fh:
                t = total_time(fh.read())
            if not t:  # None or 0.0: failed/incomplete run
                print(f"[warn] no valid time in {os.path.join(dirpath, f)}", file=sys.stderr)
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
    ap.add_argument("--scenario", help="filter by scenario (stacked, w_filters, other_shapes)")
    ap.add_argument("--dataset", help="filter by dataset (e.g. finbench, synthea_100, hetionet)")
    ap.add_argument("--csv", help="save the full table (one row per query, one column per version)")
    args = ap.parse_args()

    df = collect(args.include_ablation)
    if df.empty:
        sys.exit("No results found.")
    if args.scenario:
        df = df[df.scenario == args.scenario]
    if args.dataset:
        df = df[df.dataset == args.dataset]

    dup = df.duplicated(["scenario", "dataset", "query", "version"], keep=False)
    if dup.any():
        print(f"[warn] {dup.sum()} duplicate files for (scenario,dataset,query,version): keeping the minimum time\n",
              file=sys.stderr)
    df = df.groupby(["scenario", "dataset", "query", "version"], as_index=False)["time"].min()

    wide = df.pivot(index=["scenario", "dataset", "query"], columns="version", values="time").reset_index()
    versions = [c for c in wide.columns if c not in ("scenario", "dataset", "query")]
    wide["best_version"] = wide[versions].idxmin(axis=1)
    wide["best_time"] = wide[versions].min(axis=1)
    # second best, to see the margin
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

    print("=== Best time for each query (seconds) ===")
    out = wide[["scenario", "dataset", "query", "best_version", "best_time",
                "second_version", "second_time", "speedup_vs_2nd"]].copy()
    out["speedup_vs_2nd"] = out["speedup_vs_2nd"].round(2)
    print(out.to_string(index=False))

    print("\n=== Number of queries won per version ===")
    wins = wide.groupby(["scenario", "best_version"]).size().rename("wins").reset_index()
    tot = wide.groupby("scenario").size().rename("queries")
    wins = wins.join(tot, on="scenario")
    print(wins.sort_values(["scenario", "wins"], ascending=[True, False]).to_string(index=False))

    if args.csv:
        wide.to_csv(args.csv, sep=";", index=False)
        print(f"\nFull table saved to {args.csv}")


if __name__ == "__main__":
    main()
