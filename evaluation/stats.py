import pandas as pd
import os
import re
import ast
'''
# CSV file path
file_path = "./csv/risultati_pgkeymaker_stacked.csv"   # change to your file

# Read the CSV (separator ;)
df = pd.read_csv(file_path, sep=";")

# Make sure total_time is numeric
df["total_time"] = pd.to_numeric(df["total_time"], errors="coerce")

# Group by dataset and compute mean, min, max
stats = (
    df.groupby("dataset")["total_time"]
    .agg(media="mean", minimo="min", massimo="max")
    .reset_index()
)

# Print result
print(stats)

# (optional) save to file
stats.to_csv("./csv/statistiche_per_pgkeymaker.csv", sep=";", index=False)

for _, row in stats.iterrows():
    print(f"Dataset: {row['dataset']}")
    print(f"  Mean total_time : {row['media']:.4f}")
    print(f"  Min total_time   : {row['minimo']:.4f}")
    print(f"  Max total_time   : {row['massimo']:.4f}")
    print()

'''

file1 = "./csv/risultati_baseline_hpivalid.csv"
file2 = "./csv/risultati_pgkeymaker_hpivalid_stacked.csv"

def compute_stats(path):
    df = pd.read_csv(path, sep=";")
    df["total_time"] = pd.to_numeric(df["total_time"], errors="coerce")

    stats = (
        df.groupby("dataset")["total_time"]
        .agg(media="mean", minimo="min", massimo="max")
        .reset_index()
    )
    return stats

def compare_minimal_keys(hyucc_folder, pgmaker_folder, datasets, verbose=False):
    """
    Compares the minimal HyUCC keys with those found by PGKEYMAKER.

    Parameters
    ----------
    hyucc_folder : str
        Folder containing the results_HyUCC_bs1_* files

    pgmaker_folder : str
        Folder containing the results_PGKEYMAKER_* files

    datasets : list[str]
        List of datasets to compare.
        Example: ["Fraud", "PaySim", "IBM"]

    verbose : bool
        If True, prints the comparison details for each individual pattern.
    """

    def normalize_pattern(pattern):
        pattern = pattern.strip()
        pattern = re.sub(r"^###\s*PATTERN\s*", "", pattern)
        pattern = pattern.replace("\\:", ":")
        pattern = re.sub(r"\s+", "", pattern)
        return pattern

    def load_hyucc(file):
        """
        The actual format of the HyUCC file is:

            ### PATTERN <general pattern of the query>
            <sub-pattern1> - INTERNAL_KEYS [ ... ]
            <sub-pattern2> - INTERNAL_KEYS [ ... ]
            ...

        Each "... - INTERNAL_KEYS [...]" line has its own sub-pattern
        (different from the one given in the "### PATTERN" header), so it must be
        extracted line by line, not assuming it immediately follows
        the header.
        """
        result = {}

        with open(file, encoding="utf8") as f:
            lines = [l.strip() for l in f if l.strip()]

        for line in lines:

            if line.startswith("### PATTERN"):
                continue

            m = re.match(r"^(.*?)\s*-\s*INTERNAL_KEYS\s*(\[.*\])\s*$", line)

            if m:
                pattern = normalize_pattern(m.group(1))
                keys = ast.literal_eval(m.group(2))
                result[pattern] = {frozenset(k) for k in keys}

        return result

    def load_pgmaker(file):

        result = {}

        with open(file, encoding="utf8") as f:

            next(f)

            for line in f:

                line = line.strip()

                if not line:
                    continue

                if ";" not in line:
                    # trailing log lines (execution time, memory, etc.)
                    continue

                pattern, keys = line.split(";", 1)

                pattern = normalize_pattern(pattern)

                try:
                    parsed = ast.literal_eval(keys)
                except (SyntaxError, ValueError):
                    continue

                result[pattern] = {frozenset(k) for k in parsed}

        return result

    grand_common = 0
    grand_baseline = 0
    grand_pgmaker = 0

    print("-" * 70)

    for dataset in datasets:

        print(f"\nDATASET: {dataset}")

        dataset_common = 0
        dataset_baseline = 0
        dataset_pgmaker = 0

        for query in range(1, 9):

            hyucc_file = os.path.join(
                hyucc_folder,
                f"results_HyUCC_bs1_{dataset}_{query}.txt"
            )

            pg_file = os.path.join(
                pgmaker_folder,
                f"results_PGKEYMAKER_{dataset}_{query}.txt"
            )

            if not (os.path.exists(hyucc_file) and os.path.exists(pg_file)):
                print(f"Query {query}: missing file")
                continue

            baseline = load_hyucc(hyucc_file)
            mine = load_pgmaker(pg_file)

            baseline_count = 0
            common_count = 0
            pgmaker_count = sum(len(keys) for keys in mine.values())
            print(f"Query {query}: PGKEYMAKER total keys = {pgmaker_count}")
            for pattern in baseline:

                base = baseline[pattern]
                found = mine.get(pattern, set())

                baseline_count += len(base)
                common_count += len(base & found)

                if verbose:
                    p_perc = 100 * len(base & found) / len(base) if base else 0
                    status = "" if pattern in mine else "  [pattern missing in PGKEYMAKER]"
                    print(
                        f"    {pattern[:55]:55s} "
                        f"{len(base & found):4d}/{len(base):4d} ({p_perc:6.2f}%){status}"
                    )

            dataset_common += common_count
            dataset_baseline += baseline_count
            dataset_pgmaker += pgmaker_count

            perc = 100 * common_count / baseline_count if baseline_count else 0

            print(
                f"Query {query}: {common_count}/{baseline_count} ({perc:.2f}%)"
            )

        dataset_perc = (
            100 * dataset_common / dataset_baseline
            if dataset_baseline else 0
        )

        print(
            f"--> Dataset {dataset}: "
            f"{dataset_common}/{dataset_baseline} ({dataset_perc:.2f}%)" f"| PGKEYMAKER total: {pgmaker_count}"
        )

        grand_common += dataset_common
        grand_baseline += dataset_baseline
        grand_pgmaker += dataset_pgmaker

    print("\n" + "=" * 70)

    total_perc = (
        100 * grand_common / grand_baseline
        if grand_baseline else 0
    )

    print(f"TOTAL: {grand_common}/{grand_baseline} ({total_perc:.2f}%)")
    print(f"TOTAL PGKEYMAKER keys: {grand_pgmaker}")

    return grand_common, grand_baseline

'''
# compute statistics
stats1 = compute_stats(file1)
stats2 = compute_stats(file2)

# rename columns to tell them apart
stats1 = stats1.rename(columns={
    "media": "media_A",
    "minimo": "min_A",
    "massimo": "max_A"
})

stats2 = stats2.rename(columns={
    "media": "media_B",
    "minimo": "min_B",
    "massimo": "max_B"
})

# merge on dataset
merged = pd.merge(stats1, stats2, on="dataset")

# absolute differences
merged["diff_media"] = merged["media_B"] - merged["media_A"]
merged["diff_min"] = merged["min_B"] - merged["min_A"]
merged["diff_max"] = merged["max_B"] - merged["max_A"]

# percentage differences
merged["diff_media_%"] = (merged["diff_media"] / merged["media_A"]) * 100
merged["diff_min_%"] = (merged["diff_min"] / merged["min_A"]) * 100
merged["diff_max_%"] = (merged["diff_max"] / merged["max_A"]) * 100

print(merged)

# save
#merged.to_csv("./csv/confronto.csv", sep=";", index=False)

for _, r in merged.iterrows():
    print(f"Dataset: {r['dataset']}")
    print(f"  Mean A: {r['media_A']:.4f}")
    print(f"  Mean B: {r['media_B']:.4f}")
    print(f"  Diff   : {r['diff_media']:.4f} ({r['diff_media_%']:.2f}%)")
    print()


def compute_stats(path):
    df = pd.read_csv(path, sep=";")
    df["total_time"] = pd.to_numeric(df["total_time"], errors="coerce")

    return (
        df.groupby("dataset")["total_time"]
        .mean()
        .reset_index(name="mean_time")
    )

stats1 = compute_stats(file1).rename(columns={"mean_time": "time_A"})
stats2 = compute_stats(file2).rename(columns={"mean_time": "time_B"})

merged = pd.merge(stats1, stats2, on="dataset")

# metrics
merged["speedup"] = merged["time_A"] / merged["time_B"]
merged["improvement_%"] = (merged["time_A"] - merged["time_B"]) / merged["time_A"] * 100
merged["time_saved"] = merged["time_A"] - merged["time_B"]

# round everything to 2 decimals
merged = merged.round(2)

print(merged.sort_values("speedup", ascending=False))

# global statistics
print("\nGlobal statistics:")
print("Average speedup:", round(merged["speedup"].mean(), 2))
print("Average improvement %:", round(merged["improvement_%"].mean(), 2))
print("Average time saved:", round(merged["time_saved"].mean(), 2))

# save CSV
#output_file = "./csv/confronto_speedup.csv"
#merged.to_csv(output_file, sep=";", index=False)
'''

compare_minimal_keys(
    hyucc_folder="./scalability/baseline_hyucc/stacked",
    pgmaker_folder="./scalability/pgkeymaker_hyucc/stacked",
    datasets=["synthea_100", "synthea_200", "synthea_300", "synthea_400",
              "synthea_500", "synthea_600", "synthea_700", "synthea_800",
              "synthea_900", "synthea_1000", "finbench", "network-management","fraud"]
)