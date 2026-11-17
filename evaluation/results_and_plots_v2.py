import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import math
import numpy as np
from matplotlib.patches import Patch



def parse_filename(filename, allowed_algorithms, allowed_datasets, valid_queries=range(1, 9)):
    """
    Extracts algorithm, dataset and query from:
    results_<algorithm>_<dataset>_<query>.txt
    where algorithm and dataset may contain "_"
    """

    if not filename.startswith("results_") or not filename.endswith(".txt"):
        return None

    name = filename.replace("results_", "").replace(".txt", "")
    parts = name.split("_")

    # last part = query
    if not parts[-1].isdigit():
        return None

    query = int(parts[-1])
    if query not in valid_queries:
        return None

    body = parts[:-1]

    # try all possible splits
    for i in range(1, len(body)):
        algoritmo = "_".join(body[:i])
        dataset = "_".join(body[i:])

        if algoritmo in allowed_algorithms and dataset in allowed_datasets:
            return algoritmo, dataset, query

    return None

def create_results_file_baseline(folder, allowed_algorithms, allowed_datasets, output_csv):

    tempo_internal_re = re.compile(r"internal keys discovery time:\s*([\d\.]+)\s*s", re.IGNORECASE)
    tempo_external_re = re.compile(r"external keys discovery time:\s*([\d\.]+)\s*s", re.IGNORECASE)
    memory_re = re.compile(r"(?:max total memory|memoria massima totale):\s*([\d\.]+)\s*MB", re.IGNORECASE)

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
        memory_match = memory_re.search(content)

        tempo_internal = float(tempo_int_match.group(1)) if tempo_int_match else 0.0
        tempo_external = float(tempo_ext_match.group(1)) if tempo_ext_match else 0.0
        memory_value = float(memory_match.group(1)) if memory_match else None

        tempo_totale = tempo_internal + tempo_external

        records.append({
            "algoritmo": algoritmo,
            "dataset": dataset,
            "query": query,
            "tempo_internal": tempo_internal,
            "tempo_external": tempo_external,
            "total_time": tempo_totale,
            "memory": memory_value
        })

    df = pd.DataFrame(records)

    # optional dataset rename
    df["dataset"] = df["dataset"].replace("synthea_800_alt", "synthea_800")

    df = df.sort_values(["algoritmo", "dataset", "query"])

    df.to_csv(output_csv, sep=";", index=False)

    print(f"CSV generated: {output_csv}")
    print(df.head())

def create_results_file_stacked(folders, allowed_algorithms, allowed_datasets, output_csv,valid_queries=None, algo_aliases=None):
    """
    Builds the stacked time-breakdown csv from the result files in `folders`.
    valid_queries: queries to keep; default = 1..8 (standard) + 101, 102, 103 (meaningful).
    algo_aliases: {file_algorithm_name: csv_algorithm_name}, e.g. the hyucc meaningful files are
    named PGKEYMAKER_hyucc while the stacked ones are PGKEYMAKER -> {"PGKEYMAKER_hyucc": "PGKEYMAKER"}.
    Pass the meaningful folder too (e.g. ./scalability/pgkeymaker_hpivalid/meaningfull)
    to include 101, 102, 103.
    """
    if valid_queries is None:
        valid_queries = set(range(1, 9)) | {101, 102, 103}
    if algo_aliases is None:
        algo_aliases = {}
    parse_algorithms = list(allowed_algorithms) + [alias for alias, target in algo_aliases.items()
                                                   if target in allowed_algorithms]

    tree_re = re.compile(r"execution time for tree and leaves:\s*([\d\.]+)\s*s", re.IGNORECASE)
    external_re = re.compile(r"execution time for external keys:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case1_re = re.compile(r"execution time for case 1:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case2_re = re.compile(r"execution time for case 2:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case3_re = re.compile(r"execution time for case 3:\s*([\d\.]+)\s*s", re.IGNORECASE)
    memory_re = re.compile(r"Max memory used:\s*([\d\.]+)\s*MB", re.IGNORECASE)

    records = []

    for folder in folders:
        for file in os.listdir(folder):
            parsed = parse_filename(file, parse_algorithms, allowed_datasets, valid_queries)
            if not parsed:
                continue

            algoritmo, dataset, query = parsed
            algoritmo = algo_aliases.get(algoritmo, algoritmo)
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

    print(f"CSV generated: {output_csv}")
    print(df.head())

def create_results_file_ablation(folder1, folder2, allowed_algorithms, allowed_datasets, output_csv,algo_aliases=None):
    # Regexes for times and memory
    tree_re = re.compile(r"execution time for tree and leaves:\s*([\d\.]+)\s*s", re.IGNORECASE)
    external_re = re.compile(r"execution time for external keys:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case1_re = re.compile(r"execution time for case 1:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case2_re = re.compile(r"execution time for case 2:\s*([\d\.]+)\s*s", re.IGNORECASE)
    case3_re = re.compile(r"execution time for case 3:\s*([\d\.]+)\s*s", re.IGNORECASE)
    memory_re = re.compile(r"memory:\s*([\d\.]+)\s*MB", re.IGNORECASE)

    records = []

    # Algorithm name aliases: the hyucc other_shapes files are named PGKEYMAKER_hyucc,
    # the stacked ones PGKEYMAKER -> we merge them under the same name
    if algo_aliases is None:
        algo_aliases = {"PGKEYMAKER_hyucc": "PGKEYMAKER"}
    parse_algorithms = list(allowed_algorithms) + [alias for alias, target in algo_aliases.items()
                                                   if target in allowed_algorithms]

    # "Standard" queries (1-8) + queries of the other pattern shapes (31,32,41,42,51,52)
    ablation_queries = set(range(1, 9)) | {31, 32, 41, 42, 51, 52}

    # Folders to read from: folder1 and folder2 + the "other_shapes" folder next to each
    # (es. .../pgkeymaker_hpivalid/stacked -> .../pgkeymaker_hpivalid/other_shapes)
    folders = []
    for f in [folder1, folder2]:
        folders.append(f)
        other_shapes = os.path.join(os.path.dirname(os.path.normpath(f)), "other_shapes")
        if os.path.isdir(other_shapes):
            folders.append(other_shapes)
        else:
            print(f"Warning: folder not found, skipped: {other_shapes}")

    for folder in folders:
        for file in os.listdir(folder):
            parsed = parse_filename(file, parse_algorithms, allowed_datasets, ablation_queries)
            if not parsed:
                continue

            algoritmo, dataset, query = parsed
            algoritmo = algo_aliases.get(algoritmo, algoritmo)
            filepath = os.path.join(folder, file)

            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()

            # Extract times and memory
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

    # DataFrame creation and percentages
    df = pd.DataFrame(records)
    print(df)
    df = df.sort_values(["algoritmo", "dataset", "query"])

    time_cols = ["tree_leaves_time", "external_keys_time", "case1_time", "case2_time", "case3_time"]
    for col in time_cols:
        df[col + "_perc"] = round((df[col] / df["total_time"]) * 100, 2)

    df.to_csv(output_csv, sep=";", index=False)
    print(f"CSV generated: {output_csv}")
    print(df.head())




    #plt.show()

def plot_bar_results_from_csv_grid_ablation_other_shapes(csv_path, ncols=2, nrows=2):
    """
    Ablation plot on 4 datasets (2x2 grid), with queries 1,2,31,32,41,42,51,52
    (8 bar groups per subplot).
    """
    df = pd.read_csv(csv_path, sep=";")
    color_map = {
        "PGKEYMAKER": "#ff7f0e",
        "PGKEYMAKER (without Tree)": "#FFB06A"
    }
    df["algoritmo"] = df["algoritmo"].replace({
        "PGKEYMAKER_hpivalid": "PGKEYMAKER",
        "PGKEYMAKER_hyucc": "PGKEYMAKER",
        "PGKEYMAKER-NOTREE": "PGKEYMAKER (without Tree)"
    })

    # fixed order: row 1 = synthea_1000, finbench; row 2 = network-management, fraud
    datasets = ["synthea_1000", "finbench", "network-management", "fraud"]
    queries = [1, 2, 31, 32, 41, 42, 51, 52]
    hop_labels = ["2 hop", "3 hop", "4 hop", "5 hop"]  # one label for every 2 queries

    df = df[df["dataset"].isin(datasets) & df["query"].isin(queries)]
    algorithms = sorted(df["algoritmo"].unique())

    fig, axes = plt.subplots(nrows, ncols, figsize=(8 * ncols, 3.5 * nrows),
                             sharex=True, sharey=False)
    axes = axes.flatten()
    bar_width = 0.35
    x = list(range(len(queries)))

    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]
        for i, algo in enumerate(algorithms):
            # reindex: if a query is missing the bar stays empty (no misalignment)
            values = (df_ds[df_ds["algoritmo"] == algo]
                      .drop_duplicates("query")
                      .set_index("query")["total_time"]
                      .reindex(queries))
            ax.bar([p + i * bar_width for p in x], values.fillna(0).values,
                   width=bar_width, color=color_map.get(algo, "black"), label=algo)

        ax.set_title(dataset.capitalize(), fontsize=18)
        # a single label per query pair (same length): 2 hop, 3 hop, 4 hop, 5 hop
        pair_centers = [2 * k + 0.5 + bar_width / 2 for k in range(len(queries) // 2)]
        ax.set_xticks(pair_centers)
        ax.set_xticklabels(hop_labels, fontsize=14)
        ax.tick_params(axis="x", length=0)
        # dashed separators between one length and the next, to make the pairs clear
        for k in range(1, len(queries) // 2):
            ax.axvline(2 * k - 0.5 + bar_width / 2, color="gray", linestyle=":", linewidth=1)
        ax.set_axisbelow(True)
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)

    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)", fontsize=18)
    for ax in axes[-ncols:]:
        ax.set_xlabel("Pattern length", fontsize=18)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=18, loc="upper center",
               ncol=len(algorithms), bbox_to_anchor=(0.5, 1.07))

    plt.tight_layout()
    output_pdf = f"./plots/execution_times_barplot_{ncols}_{nrows}_ablation_other_shapes.pdf"
    fig.savefig(output_pdf, bbox_inches="tight")

def plot_bar_results_from_csv_grid_ablation_other_shapes_all_datasets(csv_path, ncols=3, nrows=None):
    """
    Same as plot_bar_results_from_csv_grid_ablation_other_shapes, but on ALL datasets
    (synthea_100..synthea_1000, finbench, network-management, fraud), with queries
    1,2,31,32,41,42,51,52 (8 bar groups per subplot).
    If nrows is None it is computed from the number of datasets and ncols.
    """
    df = pd.read_csv(csv_path, sep=";")
    color_map = {
        "PGKEYMAKER": "#ff7f0e",
        "PGKEYMAKER (without Tree)": "#FFB06A"
    }
    df["algoritmo"] = df["algoritmo"].replace({
        "PGKEYMAKER_hpivalid": "PGKEYMAKER",
        "PGKEYMAKER_hyucc": "PGKEYMAKER",
        "PGKEYMAKER-NOTREE": "PGKEYMAKER (without Tree)"
    })

    queries = [1, 2, 31, 32, 41, 42, 51, 52]
    hop_labels = ["2 hop", "3 hop", "4 hop", "5 hop"]  # one label for every 2 queries

    df = df[df["query"].isin(queries)]
    # keep only the PGKEYMAKER variants (as in the original function)
    df = df[df["algoritmo"].isin(color_map.keys())]

    # order: synthea_100 ... synthea_1000 first, then the other datasets (alphabetical)
    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return (0, int(ds.split("_")[1]), ds)
        return (1, 0, ds)

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    if nrows is None:
        nrows = math.ceil(len(datasets) / ncols)

    # grid with 2*ncols half-slots: each subplot spans 2 slots, so an incomplete
    # last row can be shifted to be centered
    fig = plt.figure(figsize=(8 * ncols, 3.5 * nrows))
    gs = fig.add_gridspec(nrows, 2 * ncols)
    axes = []
    for idx in range(len(datasets)):
        r, c = divmod(idx, ncols)
        n_in_row = min(ncols, len(datasets) - r * ncols)
        offset = ncols - n_in_row  # shift in half-slots to center the row
        axes.append(fig.add_subplot(gs[r, offset + 2 * c: offset + 2 * c + 2]))
    bar_width = 0.35
    x = list(range(len(queries)))

    for ax, dataset in zip(axes, datasets):
        df_ds = df[df["dataset"] == dataset]
        for i, algo in enumerate(algorithms):
            # reindex: if a query is missing the bar stays empty (no misalignment)
            values = (df_ds[df_ds["algoritmo"] == algo]
                      .drop_duplicates("query")
                      .set_index("query")["total_time"]
                      .reindex(queries))
            ax.bar([p + i * bar_width for p in x], values.fillna(0).values,
                   width=bar_width, color=color_map.get(algo, "black"), label=algo)

        ax.set_title(dataset.capitalize(), fontsize=18)
        # a single label per query pair (same length): 2 hop, 3 hop, 4 hop, 5 hop
        pair_centers = [2 * k + 0.5 + bar_width / 2 for k in range(len(queries) // 2)]
        ax.set_xticks(pair_centers)
        ax.set_xticklabels(hop_labels, fontsize=14)
        ax.tick_params(axis="x", length=0)
        # dotted separators between one length and the next, to make the pairs clear
        for k in range(1, len(queries) // 2):
            ax.axvline(2 * k - 0.5 + bar_width / 2, color="gray", linestyle=":", linewidth=1)
        ax.set_axisbelow(True)
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)

    # y label on every subplot that starts a row; x label on the last row
    for idx, ax in enumerate(axes):
        if idx % ncols == 0:
            ax.set_ylabel("Time (s)", fontsize=18)
    last_row_start = ((len(datasets) - 1) // ncols) * ncols
    for ax in axes[last_row_start:]:
        ax.set_xlabel("Pattern length", fontsize=18)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=18, loc="upper center",
               ncol=len(algorithms), bbox_to_anchor=(0.5, 1.03))

    plt.tight_layout()
    output_pdf = f"./plots/execution_times_barplot_{ncols}_{nrows}_ablation_other_shapes_all_datasets.pdf"
    fig.savefig(output_pdf, bbox_inches="tight")

    #plt.show()

def plot_scalability_synthea(pgkeymaker_csv, hyucc_csv, hpivalid_csv, pgk_hpivalid_csv):

    # =====================================================
    # FILE READING
    # =====================================================
    df_pg = pd.read_csv(pgkeymaker_csv, sep=";")
    df_hy = pd.read_csv(hyucc_csv, sep=";")
    df_hpi = pd.read_csv(hpivalid_csv, sep=";")
    df_pgk2 = pd.read_csv(pgk_hpivalid_csv, sep=";")

    df_pg["dataset"] = df_pg["dataset"].replace("synthea_800_alt", "synthea_800")
    df_hy["dataset"] = df_hy["dataset"].replace("synthea_800_alt", "synthea_800")

    # =====================================================
    # BASIC CLEANING
    # =====================================================
    for df in [df_pg, df_hy]:
        df.columns = df.columns.str.strip()
        df["dataset"] = df["dataset"].astype(str).str.strip()
        df["algoritmo"] = df["algoritmo"].astype(str).str.strip()

    # =====================================================
    # MERGING DATASETS
    # =====================================================
    df = pd.concat([df_pg, df_hy, df_hpi,df_pgk2], ignore_index=True)

    color_map = {
        "HyUCC": "#56B4E9",  # blue
        "PGKEYMAKER (HyUCC)": "#D55E00",  # orange
        "HPIValid": "#009E73",  # green
        "PGKEYMAKER": "#E69F00" # red
    }

    df["algoritmo"] = df["algoritmo"].replace({
        "HyUCC_bs1": "HyUCC",
        "PGKEYMAKER_hpivalid": "PGKEYMAKER",
        "PGKEYMAKER": "PGKEYMAKER (HyUCC)",
    })

    marker_map = {
        "HyUCC": "o",  # circle
        "PGKEYMAKER (HyUCC)": "^",  # triangle
        "HPIValid": "s",  # square
        "PGKEYMAKER": "D"  # diamond
    }

    print(df["algoritmo"])
    # =====================================================
    # KEEP ONLY SYNTHETIC
    # =====================================================
    df = df[df["dataset"].str.startswith("synthea_")]

    # =====================================================
    # EXTRACT SIZE
    # =====================================================
    df["size"] = df["dataset"].str.split("_").str[1].astype(int)

    print("Sizes found:", sorted(df["size"].unique()))
    print("Algorithms found:", df["algoritmo"].unique())

    # =====================================================
    # SORT
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
                marker=marker_map.get(algo, "o"),
                markersize = 6,
                linewidth=2,
                color=color_map.get(algo, "black"),  # fallback
                label=algo
            )

        ax.set_title(f"Q{query}",fontsize=25)

        sizes = sorted(df["size"].unique())

        xmin = min(sizes)
        xmax = max(sizes)
        padding = max((xmax - xmin) * 0.05, 50)

        ax.set_xlim(xmin - padding, xmax + padding)
        ax.set_xticks(sizes)

        if i >= (rows - 1) * cols:
            ax.set_xlabel("Dataset size",fontsize=25)

        ymin = df["total_time"].min()
        ymax = df["total_time"].max()
        padding = (ymax - ymin) * 0.05

        ax.set_ylim(ymin - padding, 2000 + padding)

        if i % cols == 0:
            ax.set_ylabel("Execution time (s)",fontsize=25)

        ax.grid(True, linestyle="--", alpha=0.3)

    # =====================================================
    # REMOVING EMPTY SUBPLOTS
    # =====================================================
    for i in range(n_queries, rows * cols):
        fig.delaxes(axes[i])

    # =====================================================
    # SINGLE LEGEND
    # =====================================================
    handles, labels = axes[0].get_legend_handles_labels()

    fig.legend(
        handles,
        labels,
        fontsize=22,
        loc="upper center",
        ncol=len(algorithms),
        bbox_to_anchor=(0.5, 1)
    )

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig("./plots/scalability_synthea.pdf", bbox_inches="tight")
    #plt.show()



    #plt.show()

    #plt.show()

        #plt.show()

def plot_stacked_execution_times_percentages_complete_with_meaningful(csv_path, ncols=3):
    """
    Time breakdown on all 13 datasets (fraud included), queries 1..8 + the meaningful
    ones (101, 102, 103 -> labelled M1, M2, M3 as in the table): 11 bars per subplot.
    Reads only the stacked csv, which must contain those queries
    (create_results_file_stacked with the stacked + meaningfull folders).
    Layout: ncols columns, last incomplete row centered.
    """
    base_queries = list(range(1, 9))
    meaningful_queries = [101, 102, 103]
    queries_all = base_queries + meaningful_queries
    x_labels = [str(q) for q in base_queries] + [f"M{k + 1}" for k in range(len(meaningful_queries))]

    df = pd.read_csv(csv_path, sep=";")
    df["dataset"] = df["dataset"].replace({"synthea_800_alt": "synthea_800"})
    df = df[df["query"].isin(queries_all)]

    # order: synthea_100 ... synthea_1000 first, then the other datasets (alphabetical)
    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return (0, int(ds.split("_")[1]), ds)
        return (1, 0, ds)

    allowed_datasets = [f"synthea_{n}" for n in range(100, 1001, 100)] + \
                       ["finbench", "network-management", "fraud"]
    df = df[df["dataset"].isin(allowed_datasets)]
    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)

    rows = math.ceil(len(datasets) / ncols)

    plt.rcParams["hatch.linewidth"] = 0.5

    # grid with 2*ncols half-slots: each subplot spans 2 slots, so an incomplete
    # last row can be shifted to be centered
    fig = plt.figure(figsize=(8 * ncols, 4.3 * rows))
    gs = fig.add_gridspec(rows, 2 * ncols)
    axes = []
    for idx in range(len(datasets)):
        r, c = divmod(idx, ncols)
        n_in_row = min(ncols, len(datasets) - r * ncols)
        offset = ncols - n_in_row
        axes.append(fig.add_subplot(gs[r, offset + 2 * c: offset + 2 * c + 2]))

    colors = ["#ff7f0e", "#ffbb78", "#ffd2a8", "#ffe3c8", "#fff2e6"]
    hatches = ["o", "//", "\\\\", "xx", ".."]
    labels = [
        "Tree construction and\ninternal key discovery",
        "External keys discovery",
        "Concatenation",
        "Validation",
        "Exhaustive discovery"
    ]
    perc_cols = ["tree_leaves_time_perc", "external_keys_time_perc",
                 "case1_time_perc", "case2_time_perc", "case3_time_perc"]

    for i, (ax, dataset) in enumerate(zip(axes, datasets)):
        # reindex: a missing query leaves an empty slot instead of shifting the bars
        df_ds = (df[df["dataset"] == dataset]
                 .drop_duplicates("query")
                 .set_index("query")
                 .reindex(queries_all))
        x = np.arange(len(queries_all))

        bottoms = np.zeros(len(x))
        for col, color, hatch, label in zip(perc_cols, colors, hatches, labels):
            values = df_ds[col].fillna(0).values
            ax.bar(x, values, bottom=bottoms, color=color, hatch=hatch,
                   edgecolor="black", linewidth=0.4, width=0.72, label=label)
            bottoms += values

        # separator between the standard queries (1..8) and the meaningful ones (M1..M3)
        ax.axvline(len(base_queries) - 0.5, color="gray", linestyle=":", linewidth=1)

        ax.set_title(dataset.replace("_", " "), fontsize=17)
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=14)
        ax.set_ylim(0, 100)
        ax.set_yticks(np.arange(0, 101, 20))
        ax.tick_params(axis="y", labelsize=15)
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        ax.set_axisbelow(True)

        # ylabel on the first subplot of every row
        if i % ncols == 0:
            ax.set_ylabel("Execution Time (%)", fontsize=15)

    # xlabel on the last row
    last_row_start = ((len(datasets) - 1) // ncols) * ncols
    for ax in axes[last_row_start:]:
        ax.set_xlabel("Query", fontsize=15)

    handles, legend_labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, legend_labels, loc="upper center",
               bbox_to_anchor=(0.5, 0.97), ncol=5, fontsize=15, frameon=False)

    fig.subplots_adjust(top=0.90, bottom=0.06, left=0.06, right=0.99,
                        hspace=0.5, wspace=0.8)

    fig.savefig("./plots/stacked_ex_times_complete_with_meaningful.pdf", bbox_inches="tight")


    #plt.show()

def plot_stacked_barchart_comparison_reducted_v2(csv_hyucc, csv_pg, csv_hpi,pgk_hpivalid_csv, ncols, nrows):
    # --- LOAD ---
    df_h = pd.read_csv(csv_hyucc, sep=";")
    df_p = pd.read_csv(csv_pg, sep=";")
    df_hpi = pd.read_csv(csv_hpi, sep=";")
    df_pgk2 = pd.read_csv(pgk_hpivalid_csv, sep=";")

    # normalize names
    df_h["algoritmo"] = "HyUCC"

    # --- SEGMENT COMPUTATION ---
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

    # --- DATASET FILTER ---
    df = df[df["dataset"].isin([
        "synthea_400","synthea_500","synthea_600",
        "synthea_700","synthea_800","synthea_900","synthea_1000"
    ])]

    # --- DATASET ORDERING ---
    def dataset_sort_key(ds):
        if ds.startswith("synthea_"):
            return int(ds.split("_")[1])
        return math.inf

    datasets = sorted(df["dataset"].unique(), key=dataset_sort_key)
    algorithms = sorted(df["algoritmo"].unique())

    # --- FIGURE ---
    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(22, 3),   # improved
        sharex=True,
        sharey=False
    )
    axes = axes.flatten()

    # adaptive width (essential FIX)
    bar_width = 0.8 / len(algorithms)

    # --- COLORS ---
    segment_colors = {
        "HyUCC_internal": "#56B4E9",
        "HyUCC_external": "#7EC9F1",
        "PGKEYMAKER_internal": "#D55E00",
        "PGKEYMAKER_external": "#FF9E51",
        "HPIValid_internal": "#009E73",
        "HPIValid_external": "#44B999",
        "PGKEYMAKER_hpivalid_internal": "#E69F00",
        "PGKEYMAKER_hpivalid_external": "#FFCB59",
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

            # CORRECT POSITION (centered)
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

        # corrected X axis
        ax.set_xticks(x)
        ax.set_xticklabels(queries)

        # Y
        ax.set_ylim(0, df["total"].max() * 1.1)
        ax.yaxis.set_major_locator(plt.MaxNLocator())

        # style
        ax.set_title(dataset.capitalize(), fontsize=16)
        ax.grid(True, axis="y", linestyle="--", alpha=0.7)
        ax.set_axisbelow(True)

    # hide empty subplots
    for ax in axes[len(datasets):]:
        ax.set_visible(False)

    # labels
    for ax in axes[::ncols]:
        ax.set_ylabel("Time (s)", fontsize=14)

    for ax in axes[-ncols:]:
        ax.set_xlabel("Query", fontsize=14)

    # --- LEGEND ---
    legend_elements = [
        Patch(facecolor="#56B4E9", label="internal keys (HyUCC)"),
        Patch(facecolor="#7EC9F1", label="external keys (HyUCC)"),
        Patch(facecolor="#D55E00", label="internal keys (PG-KEYMAKER w. HyUCC)"),
        Patch(facecolor="#FF9E51", label="external keys (PG-KEYMAKER w. HyUCC)"),
        Patch(facecolor="#009E73", label="internal keys (HPIValid)"),
        Patch(facecolor="#44B999", label="external keys (HPIValid)"),
        Patch(facecolor="#E69F00", label="internal keys (PG-KEYMAKER)"),
        Patch(facecolor="#FFCB59", label="external keys (PG-KEYMAKER)"),
    ]

    fig.legend(
        handles=legend_elements,
        loc="upper center",
        ncol=4,
        bbox_to_anchor=(0.5, 1.25),
        fontsize=12
    )

    plt.tight_layout()
    fig.savefig(f"./plots/stacked_barchart_comparison_reducted_{nrows}x{ncols}_fixed.pdf",bbox_inches="tight")

'''
allowed_algorithms=["HyUCC_bs1"]
allowed_datasets=["synthea_100","synthea_200","synthea_300","synthea_400",
                  "synthea_500","synthea_600","synthea_700","synthea_800",
                  "synthea_900","synthea_1000","finbench","network-management","fraud"]
create_results_file_baseline("./scalability/baseline_hyucc/stacked",allowed_algorithms,allowed_datasets,"./csv/risultati_baseline_hyucc.csv")


allowed_algorithms=["HPIValid"]
allowed_datasets=["synthea_100","synthea_200","synthea_300","synthea_400",
                  "synthea_500","synthea_600","synthea_700","synthea_800",
                  "synthea_900","synthea_1000","finbench","network-management","fraud"]
create_results_file_baseline("./scalability/baseline_hpivalid/stacked",allowed_algorithms,allowed_datasets,"./csv/risultati_baseline_hpivalid.csv")

#hyucc version
allowed_algorithms=["PGKEYMAKER"]
create_results_file_stacked(["./scalability/pgkeymaker_hyucc/stacked","./scalability/pgkeymaker_hyucc/meaningfull"],allowed_algorithms,allowed_datasets, "./csv/risultati_pgkeymaker_hyucc_stacked.csv",
                            algo_aliases={"PGKEYMAKER_hyucc": "PGKEYMAKER"})

#hpivalid version
allowed_algorithms=["PGKEYMAKER_hpivalid"]
allowed_datasets=["synthea_100","synthea_200","synthea_300","synthea_400",
                  "synthea_500","synthea_600","synthea_700","synthea_800",
                  "synthea_900","synthea_1000","finbench","network-management","fraud"]
create_results_file_stacked(["./scalability/pgkeymaker_hpivalid/stacked","./scalability/pgkeymaker_hpivalid/meaningfull"],allowed_algorithms,allowed_datasets, "./csv/risultati_pgkeymaker_hpivalid_stacked.csv")


# ablation results (include fraud + other_shapes)
_ds_all = ["synthea_100","synthea_200","synthea_300","synthea_400",
           "synthea_500","synthea_600","synthea_700","synthea_800",
           "synthea_900","synthea_1000","finbench","network-management","fraud"]
create_results_file_ablation("./scalability/pgkeymaker_hyucc/stacked","./scalability/ablation/pgkeymaker_hyucc_notree/stacked",["PGKEYMAKER","PGKEYMAKER-NOTREE"],_ds_all,"./csv/risultati_ablation_hyucc.csv")

create_results_file_ablation("./scalability/pgkeymaker_hpivalid/stacked","./scalability/ablation/pgkeymaker_hpivalid_notree/stacked",["PGKEYMAKER_hpivalid","PGKEYMAKER-NOTREE"],_ds_all,"./csv/risultati_ablation_hpivalid.csv")

create_results_file_ablation("./scalability/pgkeymaker_hpivalid/stacked","./scalability/ablation/pgkeymaker_hpivalid_notree/stacked",["PGKEYMAKER_hpivalid","PGKEYMAKER-NOTREE"],_ds_all,"./csv/risultati_ablation_hpi.csv")
'''

#plot scalability
#plot_scalability_synthea("./csv/risultati_pgkeymaker_hyucc_stacked.csv","./csv/risultati_baseline_hyucc.csv","./csv/risultati_baseline_hpivalid.csv","./csv/risultati_pgkeymaker_hpivalid_stacked.csv")

#comparison
#plot_stacked_barchart_comparison_reducted_v2("./csv/risultati_baseline.csv","./csv/risultati_pgkeymaker_stacked.csv","./csv/risultati_baseline_hpivalid.csv","./csv/risultati_pgkeymaker_hpivalid_stacked.csv", 7, 1)

#ablation study
#plot_bar_results_from_csv_grid_ablation_other_shapes("./csv/risultati_ablation_hpi.csv",2,2)
#plot_bar_results_from_csv_grid_ablation_other_shapes_all_datasets("./csv/risultati_ablation_hpi.csv")

#time breakdown: queries 1-8 + meaningful (M1-M3), all datasets (run from the evaluation folder)
#plot_stacked_execution_times_percentages_complete_with_meaningful("./csv/risultati_pgkeymaker_hpivalid_stacked.csv")
