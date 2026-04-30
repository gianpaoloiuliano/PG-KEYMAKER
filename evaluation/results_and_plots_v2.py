import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import math
import numpy as np
from matplotlib.ticker import LogLocator, LogFormatter,LogFormatterMathtext, AutoLocator, AutoMinorLocator
from matplotlib.patches import Patch


def rename_ablation_files(folder):
    for filename in os.listdir(folder):
        if "PGKEYMAKER-NOTREE-NOTREE-NOTREE" in filename:
            new_filename = filename.replace("PGKEYMAKER-NOTREE-NOTREE-NOTREE", "PGKEYMAKER-NOTREE")

            old_path = os.path.join(folder, filename)
            new_path = os.path.join(folder, new_filename)

            os.rename(old_path, new_path)
            print(f"Rinominato: {filename} -> {new_filename}")

def parse_filename(filename, allowed_algorithms, allowed_datasets):
    """
    Estrae algoritmo, dataset e query da:
    results_<algoritmo>_<dataset>_<query>.txt
    dove algoritmo e dataset possono contenere "_"
    """

    if not filename.startswith("results_") or not filename.endswith(".txt"):
        return None

    name = filename.replace("results_", "").replace(".txt", "")
    parts = name.split("_")

    # ultima parte = query
    if not parts[-1].isdigit():
        return None

    query = int(parts[-1])
    if not (1 <= query <= 8):
        return None

    body = parts[:-1]

    # prova tutte le separazioni possibili
    for i in range(1, len(body)):
        algoritmo = "_".join(body[:i])
        dataset = "_".join(body[i:])

        if algoritmo in allowed_algorithms and dataset in allowed_datasets:
            return algoritmo, dataset, query

    return None

def create_results_file_baseline(folder, allowed_algorithms, allowed_datasets, output_csv):

    tempo_internal_re = re.compile(r"internal keys discovery time:\s*([\d\.]+)\s*s", re.IGNORECASE)
    tempo_external_re = re.compile(r"external keys discovery time:\s*([\d\.]+)\s*s", re.IGNORECASE)
    memoria_re = re.compile(r"memoria massima totale:\s*([\d\.]+)\s*MB", re.IGNORECASE)

    records = []

    for file in os.listdir(folder):
        parsed = parse_filename(file, allowed_algorithms, allowed_datasets)
        print(parsed)

        if not parsed:
            continue

        algoritmo, dataset, query = parsed
        filepath = os.path.join(folder, file)

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        tempo_int_match = tempo_internal_re.search(content)
        tempo_ext_match = tempo_external_re.search(content)
        memoria_match = memoria_re.search(content)

        tempo_internal = float(tempo_int_match.group(1)) if tempo_int_match else 0.0
        tempo_external = float(tempo_ext_match.group(1)) if tempo_ext_match else 0.0
        memoria = float(memoria_match.group(1)) if memoria_match else None

        tempo_totale = tempo_internal + tempo_external

        records.append({
            "algoritmo": algoritmo,
            "dataset": dataset,
            "query": query,
            "tempo_internal": tempo_internal,
            "tempo_external": tempo_external,
            "total_time": tempo_totale,
            "memory": memoria
        })

    df = pd.DataFrame(records)

    # rename dataset opzionale
    df["dataset"] = df["dataset"].replace("synthea_800_alt", "synthea_800")

    df = df.sort_values(["algoritmo", "dataset", "query"])

    df.to_csv(output_csv, sep=";", index=False)

    print(f"CSV generato: {output_csv}")
    print(df.head())

def create_results_file_stacked(folders, allowed_algorithms, allowed_datasets, output_csv):

    tree_re = re.compile(r"execution time for tree and leaves:\s*([\d\.]+)\s*s", re.IGNORECASE)
    external_re = re.compile(r"execution time for external keys:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case1_re = re.compile(r"execution time for case 1:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case2_re = re.compile(r"execution time for case 2:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case3_re = re.compile(r"execution time for case 3:\s*([\d\.]+)\s*s", re.IGNORECASE)
    memory_re = re.compile(r"Max memory used:\s*([\d\.]+)\s*MB", re.IGNORECASE)

    records = []

    for folder in folders:
        for file in os.listdir(folder):
            parsed = parse_filename(file, allowed_algorithms, allowed_datasets)
            if not parsed:
                continue

            algoritmo, dataset, query = parsed
            filepath = os.path.join(folder, file)

            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()

            tree_time = tree_re.search(content)
            external_time = external_re.search(content)
            case1_time = case1_re.search(content)
            case2_time = case2_re.search(content)
            case3_time = case3_re.search(content)
            memory = memory_re.search(content)

            tree_val = float(tree_time.group(1)) if tree_time else 0.0
            external_val = float(external_time.group(1)) if external_time else 0.0
            case1_val = float(case1_time.group(1)) if case1_time else 0.0
            case2_val = float(case2_time.group(1)) if case2_time else 0.0
            case3_val = float(case3_time.group(1)) if case3_time else 0.0

            total_val = round(tree_val + external_val + case1_val + case2_val + case3_val,2)

            records.append({
                "algoritmo": algoritmo,
                "dataset": dataset,
                "query": query,
                "tree_leaves_time": tree_val,
                "external_keys_time": external_val,
                "case1_time": case1_val,
                "case2_time": case2_val,
                "case3_time": case3_val,
                "total_time": total_val,
                "memory": float(memory.group(1)) if memory else None
            })

    df = pd.DataFrame(records)
    df = df.sort_values(["algoritmo", "dataset", "query"])
    time_cols = [
        "tree_leaves_time",
        "external_keys_time",
        "case1_time",
        "case2_time",
        "case3_time"
    ]

    for col in time_cols:
        df[col + "_perc"] = round((df[col] / df["total_time"]) * 100, 2)
    df.to_csv(output_csv, sep=";", index=False)

    print(f"CSV generato: {output_csv}")
    print(df.head())

def create_results_file_ablation(folder1, folder2, allowed_algorithms, allowed_datasets, output_csv):
    # Regex per i tempi e la memoria
    tree_re = re.compile(r"execution time for tree and leaves:\s*([\d\.]+)\s*s", re.IGNORECASE)
    external_re = re.compile(r"execution time for external keys:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case1_re = re.compile(r"execution time for case 1:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case2_re = re.compile(r"execution time for case 2:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case3_re = re.compile(r"execution time for case 3:\s*([\d\.]+)\s*s", re.IGNORECASE)
    memory_re = re.compile(r"memory:\s*([\d\.]+)\s*MB", re.IGNORECASE)

    records = []

    # Uniamo le due cartelle in un singolo ciclo
    for folder in [folder1, folder2]:
        for file in os.listdir(folder):
            parsed = parse_filename(file, allowed_algorithms, allowed_datasets)
            if not parsed:
                continue

            algoritmo, dataset, query = parsed
            filepath = os.path.join(folder, file)

            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()

            # Estrazione tempi e memoria
            tree_val = float(tree_re.search(content).group(1)) if tree_re.search(content) else 0.0
            external_val = float(external_re.search(content).group(1)) if external_re.search(content) else 0.0
            case1_val = float(case1_re.search(content).group(1)) if case1_re.search(content) else 0.0
            case2_val = float(case2_re.search(content).group(1)) if case2_re.search(content) else 0.0
            case3_val = float(case3_re.search(content).group(1)) if case3_re.search(content) else 0.0
            memory_val = float(memory_re.search(content).group(1)) if memory_re.search(content) else None

            total_val = round(tree_val + external_val + case1_val + case2_val + case3_val, 2)

            records.append({
                "algoritmo": algoritmo,
                "dataset": dataset,
                "query": query,
                "tree_leaves_time": tree_val,
                "external_keys_time": external_val,
                "case1_time": case1_val,
                "case2_time": case2_val,
                "case3_time": case3_val,
                "total_time": total_val,
                "memory": memory_val
            })

    # Creazione DataFrame e percentuali
    df = pd.DataFrame(records)
    df = df.sort_values(["algoritmo", "dataset", "query"])

    time_cols = ["tree_leaves_time", "external_keys_time", "case1_time", "case2_time", "case3_time"]
    for col in time_cols:
        df[col + "_perc"] = round((df[col] / df["total_time"]) * 100, 2)

    df.to_csv(output_csv, sep=";", index=False)
    print(f"CSV generato: {output_csv}")
    print(df.head())

def plot_results_from_csv_grid(csv_path,ncols,nrows):
    df = pd.read_csv(csv_path, sep=";")

    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return int(ds.split("_")[1])
        return math.inf  # dataset non synthea vanno in fondo

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(4 * ncols, 3 * nrows),
        sharex=True,
        sharey=False
    )

    axes = axes.flatten()

    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        for algo in algorithms:
            df_algo = df_ds[df_ds["algoritmo"] == algo]
            df_algo = df_algo.sort_values("query")

            ax.plot(
                df_algo["query"],
                df_algo["tempo"],
                marker="o",
                label=algo
            )

        ax.set_title(dataset, fontsize=10)
        ax.set_xticks(range(1, 8))
        #ax.grid(True)
        ax.set_axisbelow(True)  # la griglia viene disegnata sotto le barre
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)  # opzioni estetiche

    # nasconde subplot inutilizzati
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # etichette assi
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)")

    for ax in axes[-ncols:]:
        ax.set_xlabel("Query")

    # legenda unica
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=len(algorithms),
        bbox_to_anchor=(0.5, 1.02)
    )

    plt.tight_layout()
    output_pdf = f"execution_times_lineplot_{ncols}_{nrows}.pdf"
    fig.savefig(output_pdf, bbox_inches="tight")
    plt.show()

def plot_bar_results_from_csv_grid(csv_path,ncols,nrows):
    df = pd.read_csv(csv_path, sep=";")
    color_map = {
        "HyUCC": "#00748f",  # blu
        "PGKEYMAKER": "#ff0000",  # rosso scuro
        "PGKEYMAKER (without Tree)":"#00748f"
    }
    df["algoritmo"] = df["algoritmo"].replace({
        "HyUCC_bs1": "HyUCC",
        "PGKEYMAKER-NOTREE":"PGKEYMAKER (without Tree)"
    })
    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return int(ds.split("_")[1])
        return math.inf  # dataset non synthea vanno in fondo

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(5 * ncols, 3 * nrows),
        sharex=True,
        sharey=False
    )

    axes = axes.flatten()
    bar_width = 0.35
    global_min = df["total_time"].min()
    global_max = df["total_time"].max()

    ymin = 10 ** math.floor(math.log10(global_min))
    ymax = 10 ** math.ceil(math.log10(global_max))

    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        queries = sorted(df_ds["query"].unique())
        x = range(len(queries))

        for i, algo in enumerate(algorithms):
            df_algo = (
                df_ds[df_ds["algoritmo"] == algo]
                .sort_values("query")
            )
            ax.set_yscale("log")
            ax.set_ylim(ymin, ymax)

            ax.yaxis.set_major_locator(LogLocator(base=10.0))
            ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10.0))
            ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=[]))
            ax.set_axisbelow(True)
            ax.bar(
                [p + i * bar_width for p in x],
                df_algo["total_time"],
                width=bar_width,
                color=color_map.get(algo, "black"),  # fallback
                label=algo
            )

        ax.set_title(dataset, fontsize=10)
        ax.set_xticks([p + bar_width / 2 for p in x])
        ax.set_xticklabels(queries)
        ax.set_axisbelow(True)  # la griglia viene disegnata sotto le barre
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)  # opzioni estetiche
        #ax.grid(True, axis="y")

    # nasconde subplot inutilizzati
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # etichette assi
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)")

    for ax in axes[-ncols:]:
        ax.set_xlabel("Query")

    # legenda unica
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=len(algorithms),
        bbox_to_anchor=(0.5, 1.03)
    )

    plt.tight_layout()
    output_pdf = f"execution_times_barplot_{ncols}_{nrows}.pdf"
    fig.savefig(output_pdf, bbox_inches="tight")
    plt.show()

def plot_bar_results_from_csv_grid_ablation(csv_path,ncols,nrows):
    df = pd.read_csv(csv_path, sep=";")
    color_map = {
        "HyUCC": "#00748f",  # blu
        "PGKEYMAKER": "#ff7f0e",  # rosso scuro
        "PGKEYMAKER (without Tree)":"#00748f"
    }
    df["algoritmo"] = df["algoritmo"].replace({
        "HyUCC_bs1": "HyUCC",
        "PGKEYMAKER-NOTREE":"PGKEYMAKER (without Tree)"
    })
    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return int(ds.split("_")[1])
        return math.inf  # dataset non synthea vanno in fondo

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(5 * ncols, 3 * nrows),
        sharex=True,
        sharey=False
    )

    axes = axes.flatten()
    bar_width = 0.35
    '''
    global_min = df["total_time"].min()
    global_max = df["total_time"].max()

    ymin = 10 ** math.floor(math.log10(global_min))
    ymax = 10 ** math.ceil(math.log10(global_max))
    '''


    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        queries = sorted(df_ds["query"].unique())
        x = range(len(queries))

        for i, algo in enumerate(algorithms):
            df_algo = (
                df_ds[df_ds["algoritmo"] == algo]
                .sort_values("query")
            )
            '''
            ax.set_yscale("linear")
            ax.yaxis.set_major_locator(LogLocator(base=10.0))
            ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10.0))
            ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=[]))
            '''
            ax.set_axisbelow(True)
            ax.bar(
                [p + i * bar_width for p in x],
                df_algo["total_time"],
                width=bar_width,
                color=color_map.get(algo, "black"),  # fallback
                label=algo
            )

        ax.set_title(dataset, fontsize=18)
        ax.set_xticks([p + bar_width / 2 for p in x])
        ax.set_xticklabels(queries)
        ax.set_axisbelow(True)  # la griglia viene disegnata sotto le barre
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)  # opzioni estetiche
        #ax.grid(True, axis="y")

    # nasconde subplot inutilizzati
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # etichette assi
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)",fontsize=18)

    for ax in axes[-ncols:]:
        ax.set_xlabel("Query",fontsize=18)

    # legenda unica
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=len(algorithms),
        bbox_to_anchor=(0.5, 1.05)
    )

    plt.tight_layout()
    output_pdf = f"./plots/execution_times_barplot_{ncols}_{nrows}_ablation.pdf"
    fig.savefig(output_pdf, bbox_inches="tight")
    #plt.show()

def plot_bar_results_from_csv_grid_ablation_reducted(csv_path,ncols,nrows):
    df = pd.read_csv(csv_path, sep=";")
    color_map = {
        "PGKEYMAKER": "#ff7f0e",
        "PGKEYMAKER (without Tree)":"#FFB06A"
    }
    df["algoritmo"] = df["algoritmo"].replace({
        "HyUCC_bs1": "HyUCC",
        "PGKEYMAKER-NOTREE":"PGKEYMAKER (without Tree)"
    })
    # Filtra solo i dataset desiderati
    allowed_datasets = ["finbench", "fraud", "synthea_100", "synthea_1000"]
    df = df[df["dataset"].isin(allowed_datasets)]

    datasets = sorted(df["dataset"].unique())
    if "synthea_1000" in datasets:
        datasets.remove("synthea_1000")
        datasets.append("synthea_1000")

    #datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(5 * ncols, 3 * nrows),
        sharex=True,
        sharey=False
    )

    axes = axes.flatten()
    bar_width = 0.35
    '''
    global_min = df["total_time"].min()
    global_max = df["total_time"].max()

    ymin = 10 ** math.floor(math.log10(global_min))
    ymax = 10 ** math.ceil(math.log10(global_max))
    '''


    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        queries = sorted(df_ds["query"].unique())
        x = range(len(queries))

        for i, algo in enumerate(algorithms):
            df_algo = (
                df_ds[df_ds["algoritmo"] == algo]
                .sort_values("query")
            )
            '''
            ax.set_yscale("linear")
            ax.yaxis.set_major_locator(LogLocator(base=10.0))
            ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10.0))
            ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=[]))
            '''
            ax.set_axisbelow(True)
            ax.bar(
                [p + i * bar_width for p in x],
                df_algo["total_time"],
                width=bar_width,
                color=color_map.get(algo, "black"),  # fallback
                label=algo
            )

        ax.set_title(dataset.capitalize(), fontsize=18)
        ax.set_xticks([p + bar_width / 2 for p in x])
        ax.set_xticklabels(queries)
        ax.set_axisbelow(True)  # la griglia viene disegnata sotto le barre
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)  # opzioni estetiche
        #ax.grid(True, axis="y")

    # nasconde subplot inutilizzati
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # etichette assi
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)",fontsize=18)

    for ax in axes[-ncols:]:
        ax.set_xlabel("Query",fontsize=18)

    # legenda unica
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        fontsize=18,
        loc="upper center",
        ncol=len(algorithms),
        bbox_to_anchor=(0.5, 1.1)
    )

    plt.tight_layout()
    output_pdf = f"./plots/execution_times_barplot_{ncols}_{nrows}_ablation_reducted.pdf"
    fig.savefig(output_pdf, bbox_inches="tight")
    #plt.show()

def plot_bar_results_from_csv_grid_ablation_reducted_3ds(csv_path,ncols,nrows):
    df = pd.read_csv(csv_path, sep=";")
    color_map = {
        "PGKEYMAKER": "#ff7f0e",
        "PGKEYMAKER (without Tree)":"#FFB06A"
    }
    df["algoritmo"] = df["algoritmo"].replace({
        "PGKEYMAKER_hpivalid": "PGKEYMAKER",
        "PGKEYMAKER-NOTREE":"PGKEYMAKER (without Tree)"
    })
    # Filtra solo i dataset desiderati
    allowed_datasets = ["finbench", "synthea_100", "synthea_1000"]
    df = df[df["dataset"].isin(allowed_datasets)]

    datasets = sorted(df["dataset"].unique())
    if "synthea_1000" in datasets:
        datasets.remove("synthea_1000")
        datasets.append("synthea_1000")

    #datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(5 * ncols, 3 * nrows),
        sharex=True,
        sharey=False
    )

    axes = axes.flatten()
    bar_width = 0.35
    '''
    global_min = df["total_time"].min()
    global_max = df["total_time"].max()

    ymin = 10 ** math.floor(math.log10(global_min))
    ymax = 10 ** math.ceil(math.log10(global_max))
    '''


    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        queries = sorted(df_ds["query"].unique())
        x = range(len(queries))

        for i, algo in enumerate(algorithms):
            df_algo = (
                df_ds[df_ds["algoritmo"] == algo]
                .sort_values("query")
            )
            '''
            ax.set_yscale("linear")
            ax.yaxis.set_major_locator(LogLocator(base=10.0))
            ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10.0))
            ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=[]))
            '''
            ax.set_axisbelow(True)
            ax.bar(
                [p + i * bar_width for p in x],
                df_algo["total_time"],
                width=bar_width,
                color=color_map.get(algo, "black"),  # fallback
                label=algo
            )

        ax.set_title(dataset.capitalize(), fontsize=18)
        ax.set_xticks([p + bar_width / 2 for p in x])
        ax.set_xticklabels(queries)
        ax.set_axisbelow(True)  # la griglia viene disegnata sotto le barre
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)  # opzioni estetiche
        #ax.grid(True, axis="y")

    # nasconde subplot inutilizzati
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # etichette assi
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)",fontsize=18)

    for ax in axes[-ncols:]:
        ax.set_xlabel("Query",fontsize=18)

    # legenda unica
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        fontsize=18,
        loc="upper center",
        ncol=len(algorithms),
        bbox_to_anchor=(0.5, 1.2)
    )

    plt.tight_layout()
    output_pdf = f"./plots/execution_times_barplot_{ncols}_{nrows}_ablation_reducted_v2.pdf"
    fig.savefig(output_pdf, bbox_inches="tight")
    #plt.show()

def plot_bar_results_from_csv_grid_reducted(csv_path, ncols, nrows):
    df = pd.read_csv(csv_path, sep=";")
    color_map = {
        "HyUCC": "#00748f",  # blu
        "PGKEYMAKER": "#ff0000"  # rosso scuro
    }
    df["algoritmo"] = df["algoritmo"].replace({
        "HyUCC_bs1": "HyUCC",
    })
    # Filtra solo i dataset desiderati
    allowed_datasets = ["finbench", "fraud", "synthea_1000"]
    df = df[df["dataset"].isin(allowed_datasets)]

    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return int(ds.split("_")[1])
        return math.inf

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(
        nrows,
        ncols,
        figsize=(5 * ncols, 3 * nrows),
        sharex=True,
        sharey=False
    )

    axes = axes.flatten()
    bar_width = 0.35

    global_min = df["total_time"].min()
    global_max = df["total_time"].max()

    ymin = 10 ** math.floor(math.log10(global_min))
    ymax = 10 ** math.ceil(math.log10(global_max))

    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        queries = sorted(df_ds["query"].unique())
        x = range(len(queries))

        for i, algo in enumerate(algorithms):
            df_algo = (
                df_ds[df_ds["algoritmo"] == algo]
                .sort_values("query")
            )

            ax.set_yscale("log")
            ax.set_ylim(ymin, ymax)

            ax.yaxis.set_major_locator(LogLocator(base=10.0))
            ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10.0))
            ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=[]))

            ax.bar(
                [p + i * bar_width for p in x],
                df_algo["total_time"],
                width=bar_width,
                color=color_map.get(algo, "black"),  # fallback
                label=algo
            )

        ax.set_title(dataset, fontsize=10)
        ax.set_xticks([p + bar_width / 2 for p in x])
        ax.set_xticklabels(queries)

        ax.set_axisbelow(True)
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)

    # Nasconde eventuali subplot inutilizzati
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # Etichette asse Y
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)")

    # Etichette asse X
    for ax in axes[-ncols:]:
        ax.set_xlabel("Query")

    # Legenda unica
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=len(algorithms),
        bbox_to_anchor=(0.5, 1.1)
    )

    plt.tight_layout()

    output_pdf = f"execution_times_barplot_reducted.pdf"
    fig.savefig(output_pdf, bbox_inches="tight")
    plt.show()

def plot_scalability_synthea(pgkeymaker_csv, hyucc_csv, hpivalid_csv, pgk_hpivalid_csv):

    # =====================================================
    # LETTURA FILE
    # =====================================================
    df_pg = pd.read_csv(pgkeymaker_csv, sep=";")
    df_hy = pd.read_csv(hyucc_csv, sep=";")
    df_hpi = pd.read_csv(hpivalid_csv, sep=";")
    df_pgk2 = pd.read_csv(pgk_hpivalid_csv, sep=";")

    df_pg["dataset"] = df_pg["dataset"].replace("synthea_800_alt", "synthea_800")
    df_hy["dataset"] = df_hy["dataset"].replace("synthea_800_alt", "synthea_800")

    # =====================================================
    # PULIZIA BASE
    # =====================================================
    for df in [df_pg, df_hy]:
        df.columns = df.columns.str.strip()
        df["dataset"] = df["dataset"].astype(str).str.strip()
        df["algoritmo"] = df["algoritmo"].astype(str).str.strip()

    # =====================================================
    # UNIONE DEI DATASET
    # =====================================================
    df = pd.concat([df_pg, df_hy, df_hpi,df_pgk2], ignore_index=True)

    color_map = {
        "HyUCC": "#1f77b4",  # blu
        "PGKEYMAKER (HyUCC)": "#ff7f0e",  # arancione
        "HPIValid": "#2ca02c",  # verde
        "PGKEYMAKER (HPIValid)": "#9467bd" # rosso
    }

    df["algoritmo"] = df["algoritmo"].replace({
        "HyUCC_bs1": "HyUCC",
        "PGKEYMAKER_hpivalid": "PGKEYMAKER (HPIValid)",
        "PGKEYMAKER": "PGKEYMAKER (HyUCC)",
    })
    print(df["algoritmo"])
    # =====================================================
    # FILTRA SOLO SYNTHETIC
    # =====================================================
    df = df[df["dataset"].str.startswith("synthea_")]

    # =====================================================
    # ESTRAI SIZE
    # =====================================================
    df["size"] = df["dataset"].str.split("_").str[1].astype(int)

    print("Size trovate:", sorted(df["size"].unique()))
    print("Algoritmi trovati:", df["algoritmo"].unique())

    # =====================================================
    # ORDINA
    # =====================================================
    df = df.sort_values(["query", "algoritmo", "size"])

    queries = sorted(df["query"].unique())
    algorithms = sorted(df["algoritmo"].unique())
    n_queries = len(queries)

    df.to_csv("./csv/dati_plot.csv",sep=";")

    # =====================================================
    # LAYOUT
    # =====================================================
    cols = 4
    rows = int(np.ceil(n_queries / cols))

    fig, axes = plt.subplots(
        rows,
        cols,
        figsize=(4.5 * cols, 4 * rows),
        sharey=True
    )

    axes = axes.flatten()

    # =====================================================
    # PLOT
    # =====================================================
    for i, (ax, query) in enumerate(zip(axes, queries)):

        df_q = df[df["query"] == query]

        for algo in algorithms:

            df_algo = df_q[df_q["algoritmo"] == algo].sort_values("size")

            if df_algo.empty:
                continue

            ax.plot(
                df_algo["size"],
                df_algo["total_time"],
                marker="o",
                linewidth=2,
                color=color_map.get(algo, "black"),  # fallback
                label=algo
            )

        ax.set_title(f"Q{query}",fontsize=18)

        sizes = sorted(df["size"].unique())

        xmin = min(sizes)
        xmax = max(sizes)
        padding = max((xmax - xmin) * 0.05, 50)

        ax.set_xlim(xmin - padding, xmax + padding)
        ax.set_xticks(sizes)

        if i >= (rows - 1) * cols:
            ax.set_xlabel("Dataset size",fontsize=18)

        ymin = df["total_time"].min()
        ymax = df["total_time"].max()
        padding = (ymax - ymin) * 0.05

        ax.set_ylim(ymin - padding, 2000 + padding)

        if i % cols == 0:
            ax.set_ylabel("Execution time (s)",fontsize=18)

        ax.grid(True, linestyle="--", alpha=0.3)

    # =====================================================
    # RIMOZIONE SUBPLOT VUOTI
    # =====================================================
    for i in range(n_queries, rows * cols):
        fig.delaxes(axes[i])

    # =====================================================
    # LEGENDA UNICA
    # =====================================================
    handles, labels = axes[0].get_legend_handles_labels()

    fig.legend(
        handles,
        labels,
        fontsize=18,
        loc="upper center",
        ncol=len(algorithms),
        bbox_to_anchor=(0.5, 1)
    )

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig("./plots/scalability_synthea.pdf", bbox_inches="tight")
    #plt.show()

def plot_stacked_execution_times(csv_path):
    """
    Crea stacked bar chart 2x6 (dataset come subplot).
    Ogni barra rappresenta una query.
    """

    # Carica dati
    df = pd.read_csv(csv_path, sep=";")
    df = df.sort_values(["dataset", "query"])

    datasets = sorted(df["dataset"].unique())

    rows = 2
    cols = 6

    fig, axes = plt.subplots(rows, cols, figsize=(24, 10))
    axes = axes.flatten()

    for i, dataset in enumerate(datasets):
        ax = axes[i]

        df_dataset = df[df["dataset"] == dataset].sort_values("query")

        queries = df_dataset["query"].astype(str)
        x = np.arange(len(queries))

        tree = df_dataset["tree_leaves_time"].values
        case1 = df_dataset["case1_time"].values
        case2 = df_dataset["case2_time"].values
        case3 = df_dataset["case3_time"].values

        ax.bar(x, tree, label="Tree + Leaves")
        ax.bar(x, case1, bottom=tree, label="Case 1")
        ax.bar(x, case2, bottom=tree + case1, label="Case 2")
        ax.bar(x, case3, bottom=tree + case1 + case2, label="Case 3")

        ax.set_title(dataset)
        ax.set_xticks(x)
        ax.set_xticklabels(queries, rotation=45)
        ax.set_ylabel("Time (s)")

    # Spegne eventuali subplot inutilizzati
    for j in range(len(datasets), rows * cols):
        fig.delaxes(axes[j])

    # Legenda globale
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4)

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.show()

def plot_stacked_execution_times_percentages(csv_path):

    df = pd.read_csv(csv_path, sep=";")
    # Rinomina dataset
    df["dataset"] = df["dataset"].replace("synthea_800_alt", "synthea_800")
    df = df.sort_values(["dataset", "query"])

    datasets = sorted(df["dataset"].unique())
    if "synthea_1000" in datasets:
        datasets.remove("synthea_1000")
        datasets.append("synthea_1000")

    rows = 2
    cols = 6
    plt.rcParams['hatch.linewidth'] = 0.2  # default ~1.0
    fig, axes = plt.subplots(rows, cols, figsize=(24, 10))
    axes = axes.flatten()

    for i, dataset in enumerate(datasets):

        ax = axes[i]
        df_dataset = df[df["dataset"] == dataset].sort_values("query")

        queries = df_dataset["query"].astype(str)
        x = np.arange(len(queries))

        tree = df_dataset["tree_leaves_time_perc"].values
        external = df_dataset["external_keys_time_perc"].values
        case1 = df_dataset["case1_time_perc"].values
        case2 = df_dataset["case2_time_perc"].values
        case3 = df_dataset["case3_time_perc"].values

        colors = [
            "#ff7f0e",  # Tree construction (principale)
            "#ffbb78",  # External keys
            "#ffd2a8",  # Concatenation (più chiaro)
            "#ffe3c8",  # Validation (ancora più chiaro)
            "#fff2e6",  # Exhaustive discovery (quasi pastel)
        ]

        hatches = [
            "o",  # Tree construction
            "//",  # External keys
            "\\\\",  # Concatenation
            "xx",  # Validation
            "..",  # Exhaustive discovery
        ]

        ax.bar(x, tree, label="Tree construction and internal key discovery on leaves", color=colors[0], hatch=hatches[0])
        ax.bar(x, external, bottom=tree, label="External keys discovery", color=colors[1], hatch=hatches[1])
        ax.bar(x, case1, bottom=tree + external, label="Concatenation", color=colors[2], hatch=hatches[2])
        ax.bar(x, case2, bottom=tree + external + case1, label="Validation", color=colors[3], hatch=hatches[3])
        ax.bar(x, case3, bottom=tree + external + case1 + case2, label="Exaustive discovery", color=colors[4], hatch=hatches[4])

        ax.set_title(dataset,fontsize=15)
        ax.set_xticks(x)
        ax.set_xticklabels(queries, rotation=45,fontsize=15)
        ax.set_ylim(0, 105)
        ax.set_yticks(np.arange(0, 101, 20))  # tick solo fino a 100

        # Y label solo nel primo grafico di ogni riga
        if i % cols == 0:
            ax.set_ylabel("Execution Time (%)",fontsize=15)

        # X label solo nella seconda riga
        if i >= cols:
            ax.set_xlabel("Query",fontsize=15)

    # Spegne eventuali subplot inutilizzati
    for j in range(len(datasets), rows * cols):
        fig.delaxes(axes[j])

    # Legenda globale
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=5,bbox_to_anchor=(0.5, 0.98),fontsize=15)

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig("./plots/stacked_ex_times.pdf", bbox_inches="tight")
    plt.show()

def plot_stacked_execution_times_percentages_reducted(csv_path):

    df = pd.read_csv(csv_path, sep=";")
    # Rinomina dataset
    df["dataset"] = df["dataset"].replace("synthea_800_alt", "synthea_800")
    df = df.sort_values(["dataset", "query"])

    # Filtra solo i dataset desiderati
    allowed_datasets = ["finbench", "fraud", "synthea_100", "synthea_1000"]
    df = df[df["dataset"].isin(allowed_datasets)]

    datasets = sorted(df["dataset"].unique())
    if "synthea_1000" in datasets:
        datasets.remove("synthea_1000")
        datasets.append("synthea_1000")

    rows = 2
    cols = 2
    plt.rcParams['hatch.linewidth'] = 0.2  # default ~1.0
    fig, axes = plt.subplots(rows, cols, figsize=(15, 10))
    axes = axes.flatten()

    for i, dataset in enumerate(datasets):

        ax = axes[i]
        df_dataset = df[df["dataset"] == dataset].sort_values("query")

        queries = df_dataset["query"].astype(str)
        x = np.arange(len(queries))

        tree = df_dataset["tree_leaves_time_perc"].values
        external = df_dataset["external_keys_time_perc"].values
        case1 = df_dataset["case1_time_perc"].values
        case2 = df_dataset["case2_time_perc"].values
        case3 = df_dataset["case3_time_perc"].values

        colors = [
            "#ff7f0e",  # Tree construction (principale)
            "#ffbb78",  # External keys
            "#ffd2a8",  # Concatenation (più chiaro)
            "#ffe3c8",  # Validation (ancora più chiaro)
            "#fff2e6",  # Exhaustive discovery (quasi pastel)
        ]

        hatches = [
            "o",  # Tree construction
            "//",  # External keys
            "\\\\",  # Concatenation
            "xx",  # Validation
            "..",  # Exhaustive discovery
        ]

        ax.bar(x, tree, label="Tree construction and internal key discovery on leaves", color=colors[0], hatch=hatches[0])
        ax.bar(x, external, bottom=tree, label="External keys discovery", color=colors[1], hatch=hatches[1])
        ax.bar(x, case1, bottom=tree + external, label="Concatenation", color=colors[2], hatch=hatches[2])
        ax.bar(x, case2, bottom=tree + external + case1, label="Validation", color=colors[3], hatch=hatches[3])
        ax.bar(x, case3, bottom=tree + external + case1 + case2, label="Exaustive discovery", color=colors[4], hatch=hatches[4])

        ax.set_title(dataset.capitalize(),fontsize=30, pad=10)
        ax.set_xticks(x)
        ax.set_xticklabels(queries, rotation=45,fontsize=22)
        ax.set_ylim(0, 105)
        ax.set_yticks(np.arange(0, 101, 20))  # tick solo fino a 100

        '''
        # Y label solo nel primo grafico di ogni riga
        if i % cols == 0:
            ax.set_ylabel("Execution Time (%)",fontsize=30)
        '''
        fig.supylabel("Execution Time (%)", fontsize=30, x=0.02, y=0.46)
        if i >= cols:
           ax.set_xlabel("Query",fontsize=30)

    # Spegne eventuali subplot inutilizzati
    for j in range(len(datasets), rows * cols):
        fig.delaxes(axes[j])

    # Legenda globale
    handles, labels = axes[0].get_legend_handles_labels()
    #fig.legend(handles, labels, loc="upper center", ncol=3,bbox_to_anchor=(0.5, 0.98),fontsize=25)
    # prima riga (2 elementi)
    fig.legend(
        handles[:2], labels[:2],
        loc="upper center",
        ncol=2,
        bbox_to_anchor=(0.5, 0.96),
        fontsize=25
    )

    # seconda riga (3 elementi)
    fig.legend(
        handles[2:], labels[2:],
        loc="upper center",
        ncol=3,
        bbox_to_anchor=(0.5, 0.90),
        fontsize=25
    )
    plt.tight_layout(rect=[0, 0, 1, 0.80])
    fig.savefig("./plots/stacked_ex_times_reducted_2x2.pdf", bbox_inches="tight")
    #plt.show()

def plot_stacked_execution_times_percentages_reducted_3ds(csv_path):

    df = pd.read_csv(csv_path, sep=";")
    # Rinomina dataset
    df["dataset"] = df["dataset"].replace("synthea_800_alt", "synthea_800")
    df = df.sort_values(["dataset", "query"])

    # Filtra solo i dataset desiderati
    allowed_datasets = ["finbench","synthea_100", "synthea_1000"]
    df = df[df["dataset"].isin(allowed_datasets)]

    datasets = sorted(df["dataset"].unique())
    if "synthea_1000" in datasets:
        datasets.remove("synthea_1000")
        datasets.append("synthea_1000")

    rows = 1
    cols = 3
    plt.rcParams['hatch.linewidth'] = 0.2  # default ~1.0
    fig, axes = plt.subplots(rows, cols, figsize=(18, 5))
    axes = axes.flatten()

    for i, dataset in enumerate(datasets):

        ax = axes[i]
        df_dataset = df[df["dataset"] == dataset].sort_values("query")

        queries = df_dataset["query"].astype(str)
        x = np.arange(len(queries))

        tree = df_dataset["tree_leaves_time_perc"].values
        external = df_dataset["external_keys_time_perc"].values
        case1 = df_dataset["case1_time_perc"].values
        case2 = df_dataset["case2_time_perc"].values
        case3 = df_dataset["case3_time_perc"].values

        colors = [
            "#ff7f0e",  # Tree construction (principale)
            "#ffbb78",  # External keys
            "#ffd2a8",  # Concatenation (più chiaro)
            "#ffe3c8",  # Validation (ancora più chiaro)
            "#fff2e6",  # Exhaustive discovery (quasi pastel)
        ]

        hatches = [
            "o",  # Tree construction
            "//",  # External keys
            "\\\\",  # Concatenation
            "xx",  # Validation
            "..",  # Exhaustive discovery
        ]

        ax.bar(x, tree, label="Tree construction and internal key discovery on leaves", color=colors[0], hatch=hatches[0])
        ax.bar(x, external, bottom=tree, label="External keys discovery", color=colors[1], hatch=hatches[1])
        ax.bar(x, case1, bottom=tree + external, label="Concatenation", color=colors[2], hatch=hatches[2])
        ax.bar(x, case2, bottom=tree + external + case1, label="Validation", color=colors[3], hatch=hatches[3])
        ax.bar(x, case3, bottom=tree + external + case1 + case2, label="Exaustive discovery", color=colors[4], hatch=hatches[4])

        ax.set_title(dataset.capitalize(),fontsize=30, pad=10)
        ax.set_xticks(x)
        ax.set_xticklabels(queries, rotation=45,fontsize=22)
        ax.set_ylim(0, 105)
        ax.set_yticks(np.arange(0, 101, 20))  # tick solo fino a 100

        '''
        # Y label solo nel primo grafico di ogni riga
        if i % cols == 0:
            ax.set_ylabel("Execution Time (%)",fontsize=30)
        '''
        fig.supylabel("Execution Time (%)", fontsize=25, x=0.02, y=0.46)
        ax.set_xlabel("Query",fontsize=30)

    # Spegne eventuali subplot inutilizzati
    for j in range(len(datasets), rows * cols):
        fig.delaxes(axes[j])

    # Legenda globale
    handles, labels = axes[0].get_legend_handles_labels()
    #fig.legend(handles, labels, loc="upper center", ncol=3,bbox_to_anchor=(0.5, 0.98),fontsize=25)
    # prima riga (2 elementi)
    fig.legend(
        handles[:2], labels[:2],
        loc="upper center",
        ncol=2,
        bbox_to_anchor=(0.5, 0.96),
        fontsize=25
    )

    # seconda riga (3 elementi)
    fig.legend(
        handles[2:], labels[2:],
        loc="upper center",
        ncol=3,
        bbox_to_anchor=(0.5, 0.85),
        fontsize=25
    )
    plt.tight_layout(rect=[0, 0, 1, 0.65])
    fig.savefig("./plots/stacked_ex_times_reducted_1x3.pdf", bbox_inches="tight")
    #plt.show()

def plot_stacked_barchart_comparison(csv_hyucc, csv_pg, ncols, nrows):
    # --- LOAD ---
    df_h = pd.read_csv(csv_hyucc, sep=";")
    df_p = pd.read_csv(csv_pg, sep=";")

    # normalizza nomi
    df_h["algoritmo"] = "HyUCC"

    # --- CALCOLO SEGMENTI ---
    df_h["internal"] = df_h["tempo_internal"]
    df_h["external"] = df_h["tempo_external"]

    df_p["internal"] = (
        df_p["tree_leaves_time"] +
        df_p["case1_time"] +
        df_p["case2_time"] +
        df_p["case3_time"]
    )
    df_p["external"] = df_p["external_keys_time"]

    # --- MERGE ---
    df = pd.concat([
        df_h[["algoritmo", "dataset", "query", "internal", "external"]],
        df_p[["algoritmo", "dataset", "query", "internal", "external"]]
    ])
    df["total"] = df["internal"] + df["external"]

    # --- ORDINAMENTO DATASET ---
    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return int(ds.split("_")[1])
        return math.inf

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(5 * ncols, 3 * nrows),
        sharex=True,
        sharey=False
    )
    axes = axes.flatten()
    bar_width = 0.35

    # --- LIMITI LOG GLOBALI ---
    global_min = df["total"].min()
    global_max = df["total"].max()
    ymin = 10 ** math.floor(math.log10(global_min))
    ymax = 10 ** math.ceil(math.log10(global_max))

    # --- COLORI SEGMENTI UNIVOCI ---
    segment_colors = {
        "HyUCC_internal": "#1f77b4",        # blu
        "HyUCC_external": "#aec7e8",        # azzurro chiaro
        "PGKEYMAKER_internal": "#ff7f0e",   # arancio
        "PGKEYMAKER_external": "#ffbb78"    # arancio chiaro
    }

    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        queries = sorted(df_ds["query"].unique())
        x = np.arange(len(queries))

        for i, algo in enumerate(algorithms):
            df_algo = df_ds[df_ds["algoritmo"] == algo].sort_values("query")
            internal = df_algo["internal"].values
            external = df_algo["external"].values
            xpos = x + i * bar_width

            # INTERNAL
            ax.bar(
                xpos,
                internal,
                width=bar_width,
                color=segment_colors[f"{algo}_internal"],
                label=f"{algo} internal" if (dataset == datasets[0] and i == 0) else ""
            )
            # EXTERNAL
            ax.bar(
                xpos,
                external,
                width=bar_width,
                bottom=internal,
                color=segment_colors[f"{algo}_external"],
                label=f"{algo} external" if (dataset == datasets[0] and i == 0) else ""
            )

        # SCALA LOG
        ax.set_yscale("log")
        ax.set_ylim(ymin, ymax)
        ax.yaxis.set_major_locator(LogLocator(base=10.0))
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10.0))
        ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=[]))

        ax.set_title(dataset, fontsize=10)
        ax.set_xticks(x + bar_width / 2)
        ax.set_xticklabels(queries)
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)
        ax.set_axisbelow(True)

    # nascondi subplot vuoti
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # labels
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)")
    for ax in axes[-ncols:]:
        ax.set_xlabel("Query")
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor="#1f77b4", label="HyUCC internal"),
        Patch(facecolor="#aec7e8", label="HyUCC external"),
        Patch(facecolor="#ff7f0e", label="PGKEYMAKER internal"),
        Patch(facecolor="#ffbb78", label="PGKEYMAKER external"),
    ]

    fig.legend(
        handles=legend_elements,
        loc="upper center",
        ncol=4,
        bbox_to_anchor=(0.5, 1.05)
    )

    plt.tight_layout()
    fig.savefig("./plots/stacked_barchart_comparison.pdf", bbox_inches="tight")
    plt.show()

def plot_stacked_barchart_comparison_reducted(csv_hyucc, csv_pg, csv_hpi,pgk_hpivalid_csv, ncols, nrows):
    # --- LOAD ---
    df_h = pd.read_csv(csv_hyucc, sep=";")
    df_p = pd.read_csv(csv_pg, sep=";")
    df_hpi = pd.read_csv(csv_hpi, sep=";")
    df_pgk2 = pd.read_csv(pgk_hpivalid_csv, sep=";")

    # normalizza nomi
    df_h["algoritmo"] = "HyUCC"

    # --- CALCOLO SEGMENTI ---
    df_h["internal"] = df_h["tempo_internal"]
    df_h["external"] = df_h["tempo_external"]
    df_hpi["internal"] = df_hpi["tempo_internal"]
    df_hpi["external"] = df_hpi["tempo_external"]

    df_p["internal"] = (
        df_p["tree_leaves_time"] +
        df_p["case1_time"] +
        df_p["case2_time"] +
        df_p["case3_time"]
    )

    df_pgk2["internal"] = (
            df_pgk2["tree_leaves_time"] +
            df_pgk2["case1_time"] +
            df_pgk2["case2_time"] +
            df_pgk2["case3_time"]
    )

    df_p["external"] = df_p["external_keys_time"]
    df_pgk2["external"] = df_pgk2["external_keys_time"]


    # --- MERGE ---
    df = pd.concat([
        df_h[["algoritmo", "dataset", "query", "internal", "external"]],
        df_hpi[["algoritmo", "dataset", "query", "internal", "external"]],
        df_p[["algoritmo", "dataset", "query", "internal", "external"]],
        df_pgk2[["algoritmo", "dataset", "query", "internal", "external"]]
    ])
    df["total"] = df["internal"] + df["external"]
    # --- FILTRO DATASET ---
    #df = df[df["dataset"].isin(["fraud", "finbench","synthea_100", "synthea_1000"])]
    df = df[df["dataset"].isin(["synthea_400","synthea_500","synthea_600","synthea_700","synthea_800","synthea_900", "synthea_1000"])]
    # --- ORDINAMENTO DATASET -
    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return int(ds.split("_")[1])
        return math.inf

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(25,5),
        sharex=True,
        sharey=False
    )
    axes = axes.flatten()
    bar_width = 0.35

    # --- LIMITI LOG GLOBALI ---
    global_min = df["total"].min()
    global_max = df["total"].max()
    ymin = 10 ** math.floor(math.log10(global_min))
    ymax = 10 ** math.ceil(math.log10(global_max))

    # --- COLORI SEGMENTI UNIVOCI ---
    segment_colors = {
        "HyUCC_internal": "#1f77b4",        # blu
        "HyUCC_external": "#aec7e8",        # azzurro chiaro
        "PGKEYMAKER_internal": "#ff7f0e",   # arancio
        "PGKEYMAKER_external": "#ffbb78",    # arancio chiaro
        "HPIValid_internal": "#2ca02c",  # verde (nuova baseline)
        "HPIValid_external": "#78D278",  # verde (nuova baseline)
        "PGKEYMAKER_hpivalid_internal": "#FF0E0E",  # rosso
        "PGKEYMAKER_hpivalid_external": "#FF6A6A",  # rosso chiaro
    }

    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        queries = sorted(df_ds["query"].unique())
        x = np.arange(len(queries))

        for i, algo in enumerate(algorithms):
            df_algo = df_ds[df_ds["algoritmo"] == algo].sort_values("query")
            internal = df_algo["internal"].values
            external = df_algo["external"].values
            xpos = x + i * bar_width

            # INTERNAL
            ax.bar(
                xpos,
                internal,
                width=bar_width,
                color=segment_colors[f"{algo}_internal"],
                label=f"{algo} internal" if (dataset == datasets[0] and i == 0) else ""
            )
            # EXTERNAL
            ax.bar(
                xpos,
                external,
                width=bar_width,
                bottom=internal,
                color=segment_colors[f"{algo}_external"],
                label=f"{algo} external" if (dataset == datasets[0] and i == 0) else ""
            )
        '''
        # SCALA LOG
        ax.set_yscale("log")
        ax.set_ylim(ymin, ymax)
        ax.yaxis.set_major_locator(LogLocator(base=10.0))
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10.0))
        ax.yaxis.set_minor_locator(LogLocator(base=10.0, subs=[]))
        '''
        ax.set_ylim(0, df["total"].max() * 1.1)  # un 10% sopra il massimo
        ax.yaxis.set_major_locator(plt.MaxNLocator(integer=False))  # tick automatici

        ax.set_title(dataset.capitalize(), fontsize=18)
        ax.set_xticks(x + bar_width / 2)
        ax.set_xticklabels(queries)
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)
        ax.set_axisbelow(True)

    # nascondi subplot vuoti
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # labels
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)", fontsize=18)
    for ax in axes[-ncols:]:
        ax.set_xlabel("Query",fontsize=18)
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor="#1f77b4", label="internal keys (HyUCC)"),
        Patch(facecolor="#aec7e8", label="external keys (HyUCC)"),
        Patch(facecolor="#ff7f0e", label="internal keys (PG-KEYMAKER with HyUCC)"),
        Patch(facecolor="#ffbb78", label="external keys (PG-KEYMAKER with HyUCC)"),
        Patch(facecolor="#2ca02c", label="internal keys (HPIValid)"),
        Patch(facecolor="#78D278", label="external keys (HPIValid)"),
        Patch(facecolor="#FF0E0E", label="internal keys (PG-KEYMAKER with HPIValid)"),
        Patch(facecolor="#FF6A6A", label="external keys (PG-KEYMAKER with HPIValid)"),
    ]

    fig.legend(
        handles=legend_elements,
        loc="upper center",
        ncol=4,
        bbox_to_anchor=(0.5, 1.27),
        fontsize = 15
    )

    plt.tight_layout()
    fig.savefig(f"./plots/stacked_barchart_comparison_reducted_{nrows}x{ncols}_modified.pdf", bbox_inches="tight")
    #plt.show()

def plot_stacked_barchart_comparison_reducted_v2(csv_hyucc, csv_pg, csv_hpi,pgk_hpivalid_csv, ncols, nrows):
    # --- LOAD ---
    df_h = pd.read_csv(csv_hyucc, sep=";")
    df_p = pd.read_csv(csv_pg, sep=";")
    df_hpi = pd.read_csv(csv_hpi, sep=";")
    df_pgk2 = pd.read_csv(pgk_hpivalid_csv, sep=";")

    # normalizza nomi
    df_h["algoritmo"] = "HyUCC"

    # --- CALCOLO SEGMENTI ---
    df_h["internal"] = df_h["tempo_internal"]
    df_h["external"] = df_h["tempo_external"]

    df_hpi["internal"] = df_hpi["tempo_internal"]
    df_hpi["external"] = df_hpi["tempo_external"]

    df_p["internal"] = (
        df_p["tree_leaves_time"]
        + df_p["case1_time"]
        + df_p["case2_time"]
        + df_p["case3_time"]
    )

    df_pgk2["internal"] = (
        df_pgk2["tree_leaves_time"]
        + df_pgk2["case1_time"]
        + df_pgk2["case2_time"]
        + df_pgk2["case3_time"]
    )

    df_p["external"] = df_p["external_keys_time"]
    df_pgk2["external"] = df_pgk2["external_keys_time"]

    # --- MERGE ---
    df = pd.concat([
        df_h[["algoritmo", "dataset", "query", "internal", "external"]],
        df_hpi[["algoritmo", "dataset", "query", "internal", "external"]],
        df_p[["algoritmo", "dataset", "query", "internal", "external"]],
        df_pgk2[["algoritmo", "dataset", "query", "internal", "external"]],
    ])

    df["total"] = df["internal"] + df["external"]

    # --- FILTRO DATASET ---
    df = df[df["dataset"].isin([
        "synthea_400","synthea_500","synthea_600",
        "synthea_700","synthea_800","synthea_900","synthea_1000"
    ])]

    # --- ORDINAMENTO DATASET ---
    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return int(ds.split("_")[1])
        return math.inf

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    # --- FIGURA ---
    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(22, 3),   # migliorato
        sharex=True,
        sharey=False
    )
    axes = axes.flatten()

    # larghezza adattiva (FIX fondamentale)
    bar_width = 0.8 / len(algorithms)

    # --- COLORI ---
    segment_colors = {
        "HyUCC_internal": "#1f77b4",
        "HyUCC_external": "#aec7e8",
        "PGKEYMAKER_internal": "#ff7f0e",
        "PGKEYMAKER_external": "#ffbb78",
        "HPIValid_internal": "#2ca02c",
        "HPIValid_external": "#78D278",
        "PGKEYMAKER_hpivalid_internal": "#9467bd",
        "PGKEYMAKER_hpivalid_external": "#c5b0d5",
    }

    # --- PLOT ---
    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]

        queries = sorted(df_ds["query"].unique())
        x = np.arange(len(queries))

        for i, algo in enumerate(algorithms):
            df_algo = df_ds[df_ds["algoritmo"] == algo].sort_values("query")

            internal = df_algo["internal"].values
            external = df_algo["external"].values

            # POSIZIONE CORRETTA (centrata)
            xpos = x + (i - len(algorithms)/2) * bar_width + bar_width/2

            # INTERNAL
            ax.bar(
                xpos,
                internal,
                width=bar_width,
                color=segment_colors[f"{algo}_internal"]
            )

            # EXTERNAL (stacked)
            ax.bar(
                xpos,
                external,
                width=bar_width,
                bottom=internal,
                color=segment_colors[f"{algo}_external"]
            )

        # asse X corretto
        ax.set_xticks(x)
        ax.set_xticklabels(queries)

        # Y
        ax.set_ylim(0, df["total"].max() * 1.1)
        ax.yaxis.set_major_locator(plt.MaxNLocator())

        # stile
        ax.set_title(dataset.capitalize(), fontsize=16)
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)
        ax.set_axisbelow(True)

    # nascondi subplot vuoti
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # labels
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)", fontsize=14)

    for ax in axes[-ncols:]:
        ax.set_xlabel("Query", fontsize=14)

    # --- LEGENDA ---
    legend_elements = [
        Patch(facecolor="#1f77b4", label="internal keys (HyUCC)"),
        Patch(facecolor="#aec7e8", label="external keys (HyUCC)"),
        Patch(facecolor="#ff7f0e", label="internal keys (PG-KEYMAKER with HyUCC)"),
        Patch(facecolor="#ffbb78", label="external keys (PG-KEYMAKER with HyUCC)"),
        Patch(facecolor="#2ca02c", label="internal keys (HPIValid)"),
        Patch(facecolor="#78D278", label="external keys (HPIValid)"),
        Patch(facecolor="#9467bd", label="internal keys (PG-KEYMAKER with HPIValid)"),
        Patch(facecolor="#c5b0d5", label="external keys (PG-KEYMAKER with HPIValid)"),
    ]

    fig.legend(
        handles=legend_elements,
        loc="upper center",
        ncol=4,
        bbox_to_anchor=(0.5, 1.25),
        fontsize=12
    )

    plt.tight_layout()

    fig.savefig(
        f"./plots/stacked_barchart_comparison_reducted_{nrows}x{ncols}_fixed.pdf",
        bbox_inches="tight"
    )

allowed_algorithms=["HyUCC_bs1"]
allowed_datasets=["synthea_100","synthea_200","synthea_300","synthea_400",
                  "synthea_500","synthea_600","synthea_700","synthea_800",
                  "synthea_900","synthea_1000","finbench"]
create_results_file_baseline("./scalability/ucc_baseline1/stacked",allowed_algorithms,allowed_datasets,"./csv/risultati_baseline.csv")

#hyucc version
allowed_algorithms=["PGKEYMAKER"]
create_results_file_stacked(["./scalability/pgkeymaker/stacked_results"],allowed_algorithms,allowed_datasets, "./csv/risultati_pgkeymaker_stacked.csv")
allowed_algorithms=["PGKEYMAKER","PGKEYMAKER-NOTREE"]
create_results_file_ablation("./scalability/pgkeymaker/stacked_results","./scalability/ablation/pgkeymaker_hyucc_notree",allowed_algorithms,allowed_datasets,"./csv/risultati_ablation_hyucc.csv",)

allowed_algorithms=["HPIValid"]
allowed_datasets=["synthea_100","synthea_200","synthea_300","synthea_400",
                  "synthea_500","synthea_600","synthea_700","synthea_800",
                  "synthea_900","synthea_1000","finbench"]
create_results_file_baseline("./scalability/baseline_hpivalid/stacked",allowed_algorithms,allowed_datasets,"./csv/risultati_baseline_hpivalid.csv")

#hpivalid version
allowed_algorithms=["PGKEYMAKER_hpivalid"]
allowed_datasets=["synthea_100","synthea_200","synthea_300","synthea_400",
                  "synthea_500","synthea_600","synthea_700","synthea_800",
                  "synthea_900","synthea_1000","finbench"]
create_results_file_stacked(["./scalability/pgkeymaker_hpivalid/stacked"],allowed_algorithms,allowed_datasets, "./csv/risultati_pgkeymaker_hpivalid_stacked.csv")

allowed_algorithms=["PGKEYMAKER_hpivalid","PGKEYMAKER-NOTREE"]
allowed_datasets=["synthea_100","synthea_200","synthea_300","synthea_400",
                  "synthea_500","synthea_600","synthea_700","synthea_800",
                  "synthea_900","synthea_1000","finbench"]
create_results_file_ablation("./scalability/pgkeymaker_hpivalid/stacked","./scalability/ablation/pgkeymaker_hpivalid_notree",allowed_algorithms,allowed_datasets,"./csv/risultati_ablation_hpivalid.csv",)



#allowed_algorithms=["PGKEYMAKER","PGKEYMAKER-NOTREE"]
#create_results_file_ablation("./scalability/pgkeymaker/stacked_results","./scalability/ablation",allowed_algorithms,allowed_datasets,"./csv/risultati_ablation.csv",)



#plot scalability
#plot_scalability_synthea("./csv/risultati_pgkeymaker_stacked.csv","./csv/risultati_baseline.csv","./csv/risultati_baseline_hpivalid.csv","./csv/risultati_pgkeymaker_hpivalid_stacked.csv")

#comparison
#plot_stacked_barchart_comparison_reducted_v2("./csv/risultati_baseline.csv","./csv/risultati_pgkeymaker_stacked.csv","./csv/risultati_baseline_hpivalid.csv","./csv/risultati_pgkeymaker_hpivalid_stacked.csv", 7, 1)


#stacked results PGKEYMAKER
#plot_stacked_execution_times_percentages("./csv/risultati_pgkeymaker_stacked.csv")
#plot_stacked_execution_times_percentages_reducted_3ds("./csv/risultati_pgkeymaker_hpivalid_stacked.csv")

#ablation study
#plot_bar_results_from_csv_grid_ablation("./csv/risultati_ablation.csv",4,3)
#plot_bar_results_from_csv_grid_ablation_reducted("./csv/risultati_ablation.csv",2,2)
#plot_bar_results_from_csv_grid_ablation_reducted_3ds("./csv/risultati_ablation_hpi.csv",3,1)

#plot_stacked_barchart_comparison("./csv/risultati_baseline.csv","./csv/risultati_pgkeymaker_stacked.csv", 4, 3)
#plot_bar_results_from_csv_grid("risultati_scalability_final.csv",3,4)
#plot_bar_results_from_csv_grid_reducted("risultati_scalability_final.csv",3,1)
#plot_memory_bar_results_from_csv_grid_reducted("risultati_scalability_final.csv",3,1)



#plot_scalability_memory_synthea("risultati_scalability_stacked.csv","risultati_scalability_final.csv")

#ris_baseline = [['pro.code', 'en.end'], ['pro.description', 'en.end'], ['pro.code', 'en.id'], ['pro.description', 'en.id'], ['en.date', 'en.code', 'pro.code', 'en.coveredAmount'], ['en.date', 'p.marital', 'pro.code', 'en.description', 'en.coveredAmount'], ['en.date', 'en.baseCost', 'p.marital', 'pro.code', 'en.coveredAmount'], ['en.date', 'p.marital', 'pro.code', 'en.coveredAmount', 'en.class'], ['en.date', 'en.code', 'pro.description', 'en.coveredAmount'], ['en.date', 'p.marital', 'pro.description', 'en.description', 'en.coveredAmount'], ['en.date', 'en.baseCost', 'p.marital', 'pro.description', 'en.coveredAmount'], ['en.date', 'p.marital', 'pro.description', 'en.coveredAmount', 'en.class'], ['en.date', 'pro.code', 'en.claimCost'], ['en.date', 'pro.description', 'en.claimCost'], ['en.date', 'p.id', 'en.code', 'pro.code'], ['en.date', 'p.id', 'pro.code', 'en.description'], ['en.date', 'en.baseCost', 'p.id', 'pro.code'], ['en.date', 'p.id', 'pro.code', 'en.class'], ['en.date', 'en.code', 'p.SSN', 'pro.code'], ['en.date', 'p.SSN', 'pro.code', 'en.description'], ['en.date', 'en.baseCost', 'p.SSN', 'pro.code'], ['en.date', 'p.SSN', 'pro.code', 'en.class'], ['en.date', 'p.firstName', 'en.code', 'pro.code'], ['en.date', 'p.firstName', 'pro.code', 'en.description'], ['en.date', 'en.baseCost', 'p.firstName', 'pro.code'], ['en.date', 'p.firstName', 'pro.code', 'en.class'], ['en.date', 'p.lastName', 'en.code', 'pro.code'], ['en.date', 'p.lastName', 'pro.code', 'en.description'], ['en.date', 'en.baseCost', 'p.lastName', 'pro.code'], ['en.date', 'p.lastName', 'pro.code', 'en.class'], ['en.date', 'p.deathDate', 'en.code', 'pro.code'], ['en.date', 'p.deathDate', 'pro.code', 'en.description'], ['en.date', 'en.baseCost', 'p.deathDate', 'pro.code'], ['en.date', 'p.deathDate', 'pro.code', 'en.class'], ['en.date', 'p.id', 'en.code', 'pro.description'], ['en.date', 'p.id', 'pro.description', 'en.description'], ['en.date', 'en.baseCost', 'p.id', 'pro.description'], ['en.date', 'p.id', 'pro.description', 'en.class'], ['en.date', 'en.code', 'p.SSN', 'pro.description'], ['en.date', 'p.SSN', 'pro.description', 'en.description'], ['en.date', 'en.baseCost', 'p.SSN', 'pro.description'], ['en.date', 'p.SSN', 'pro.description', 'en.class'], ['en.date', 'p.firstName', 'en.code', 'pro.description'], ['en.date', 'p.firstName', 'pro.description', 'en.description'], ['en.date', 'en.baseCost', 'p.firstName', 'pro.description'], ['en.date', 'p.firstName', 'pro.description', 'en.class'], ['en.date', 'p.lastName', 'en.code', 'pro.description'], ['en.date', 'p.lastName', 'pro.description', 'en.description'], ['en.date', 'en.baseCost', 'p.lastName', 'pro.description'], ['en.date', 'p.lastName', 'pro.description', 'en.class'], ['en.date', 'p.deathDate', 'en.code', 'pro.description'], ['en.date', 'p.deathDate', 'pro.description', 'en.description'], ['en.date', 'en.baseCost', 'p.deathDate', 'pro.description'], ['en.date', 'p.deathDate', 'pro.description', 'en.class']]
#ris_pg = [['en.date', 'en.code', 'en.coveredAmount', 'pro.code'], ['en.date', 'en.claimCost', 'pro.code'], ['en.end', 'pro.code'], ['en.id', 'pro.code'], ['en.date', 'en.claimCost', 'pro.description'], ['en.date', 'en.code', 'en.coveredAmount', 'pro.description'], ['en.end', 'pro.description'], ['en.id', 'pro.description']]
#print("numero chiavi baseline:",len(ris_baseline))
#print("numero chiavi pg:",len(ris_pg))