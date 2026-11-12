import pandas as pd
import os
import re
import ast
'''
# Percorso del file CSV
file_path = "./csv/risultati_pgkeymaker_stacked.csv"   # cambia con il tuo file

# Legge il CSV (separatore ;)
df = pd.read_csv(file_path, sep=";")

# Assicurati che total_time sia numerico
df["total_time"] = pd.to_numeric(df["total_time"], errors="coerce")

# Raggruppa per dataset e calcola media, min, max
stats = (
    df.groupby("dataset")["total_time"]
    .agg(media="mean", minimo="min", massimo="max")
    .reset_index()
)

# Stampa risultato
print(stats)

# (opzionale) salva su file
stats.to_csv("./csv/statistiche_per_pgkeymaker.csv", sep=";", index=False)

for _, row in stats.iterrows():
    print(f"Dataset: {row['dataset']}")
    print(f"  Media total_time : {row['media']:.4f}")
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
    Confronta le chiavi minimali HyUCC con quelle trovate da PGKEYMAKER.

    Parameters
    ----------
    hyucc_folder : str
        Cartella contenente i file results_HyUCC_bs1_*

    pgmaker_folder : str
        Cartella contenente i file results_PGKEYMAKER_*

    datasets : list[str]
        Lista dei dataset da confrontare.
        Esempio: ["Fraud", "PaySim", "IBM"]

    verbose : bool
        Se True stampa il dettaglio del confronto per ogni singolo pattern.
    """

    def normalize_pattern(pattern):
        pattern = pattern.strip()
        pattern = re.sub(r"^###\s*PATTERN\s*", "", pattern)
        pattern = pattern.replace("\\:", ":")
        pattern = re.sub(r"\s+", "", pattern)
        return pattern

    def load_hyucc(file):
        """
        Il formato reale del file HyUCC è:

            ### PATTERN <pattern generale della query>
            <sotto-pattern1> - INTERNAL_KEYS [ ... ]
            <sotto-pattern2> - INTERNAL_KEYS [ ... ]
            ...

        Ogni riga "... - INTERNAL_KEYS [...]" ha il proprio sotto-pattern
        (diverso da quello indicato nell'header "### PATTERN"), quindi va
        estratto riga per riga, non assumendo che segua immediatamente
        l'header.
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
                    # righe finali di log (execution time, memoria, ecc.)
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
                print(f"Query {query}: file mancante")
                continue

            baseline = load_hyucc(hyucc_file)
            mine = load_pgmaker(pg_file)

            baseline_count = 0
            common_count = 0
            pgmaker_count = sum(len(keys) for keys in mine.values())
            print(f"Query {query}: PGKEYMAKER chiavi totali = {pgmaker_count}")
            for pattern in baseline:

                base = baseline[pattern]
                found = mine.get(pattern, set())

                baseline_count += len(base)
                common_count += len(base & found)

                if verbose:
                    p_perc = 100 * len(base & found) / len(base) if base else 0
                    status = "" if pattern in mine else "  [pattern assente in PGKEYMAKER]"
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
            f"{dataset_common}/{dataset_baseline} ({dataset_perc:.2f}%)" f"| PGKEYMAKER totale: {pgmaker_count}"
        )

        grand_common += dataset_common
        grand_baseline += dataset_baseline
        grand_pgmaker += dataset_pgmaker

    print("\n" + "=" * 70)

    total_perc = (
        100 * grand_common / grand_baseline
        if grand_baseline else 0
    )

    print(f"TOTALE: {grand_common}/{grand_baseline} ({total_perc:.2f}%)")
    print(f"TOTALE chiavi PGKEYMAKER: {grand_pgmaker}")

    return grand_common, grand_baseline

'''
# calcola statistiche
stats1 = compute_stats(file1)
stats2 = compute_stats(file2)

# rinomina colonne per distinguere
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

# merge sui dataset
merged = pd.merge(stats1, stats2, on="dataset")

# differenze assolute
merged["diff_media"] = merged["media_B"] - merged["media_A"]
merged["diff_min"] = merged["min_B"] - merged["min_A"]
merged["diff_max"] = merged["max_B"] - merged["max_A"]

# differenze percentuali
merged["diff_media_%"] = (merged["diff_media"] / merged["media_A"]) * 100
merged["diff_min_%"] = (merged["diff_min"] / merged["min_A"]) * 100
merged["diff_max_%"] = (merged["diff_max"] / merged["max_A"]) * 100

print(merged)

# salva
#merged.to_csv("./csv/confronto.csv", sep=";", index=False)

for _, r in merged.iterrows():
    print(f"Dataset: {r['dataset']}")
    print(f"  Media A: {r['media_A']:.4f}")
    print(f"  Media B: {r['media_B']:.4f}")
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

# metriche
merged["speedup"] = merged["time_A"] / merged["time_B"]
merged["improvement_%"] = (merged["time_A"] - merged["time_B"]) / merged["time_A"] * 100
merged["time_saved"] = merged["time_A"] - merged["time_B"]

# arrotonda tutto a 2 decimali
merged = merged.round(2)

print(merged.sort_values("speedup", ascending=False))

# statistiche globali
print("\nStatistiche globali:")
print("Speedup medio:", round(merged["speedup"].mean(), 2))
print("Miglioramento medio %:", round(merged["improvement_%"].mean(), 2))
print("Tempo medio risparmiato:", round(merged["time_saved"].mean(), 2))

# salva CSV
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