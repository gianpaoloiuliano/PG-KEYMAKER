import pandas as pd
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
file2 = "./csv/risultati_pgkeymaker_stacked.csv"

def compute_stats(path):
    df = pd.read_csv(path, sep=";")
    df["total_time"] = pd.to_numeric(df["total_time"], errors="coerce")

    stats = (
        df.groupby("dataset")["total_time"]
        .agg(media="mean", minimo="min", massimo="max")
        .reset_index()
    )
    return stats

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
