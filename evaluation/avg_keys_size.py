import os
import ast

def avg_baseline(folder_path,allowed_algorithms,allowed_datasets,allowed_queries):
    dataset_query_sum = {}
    dataset_query_count = {}

    dataset_total_sum = {}
    dataset_total_count = {}


    for dataset in allowed_datasets:

        dataset_query_sum[dataset] = {}
        dataset_query_count[dataset] = {}
        dataset_total_sum[dataset] = 0
        dataset_total_count[dataset] = 0

        for query in allowed_queries:

            dataset_query_sum[dataset][query] = 0
            dataset_query_count[dataset][query] = 0

            # costruzione filename
            filename = "results_" + allowed_algorithms[0] + "_" + dataset + "_" + query + ".txt"
            file_path = os.path.join(folder_path, filename)
            print(file_path)
            # se il file non esiste, salta
            if not os.path.exists(file_path):
                continue

            total_size = 0
            total_keys = 0

            with open(file_path, "r") as f:
                for line in f:
                    if "INTERNAL_KEYS" in line:
                        keys_part = line.split("INTERNAL_KEYS")[1].strip()
                        keys = ast.literal_eval(keys_part)

                        for key in keys:
                            total_size += len(key)
                            total_keys += 1

            # accumulo
            dataset_query_sum[dataset][query] += total_size
            dataset_query_count[dataset][query] += total_keys

            dataset_total_sum[dataset] += total_size
            dataset_total_count[dataset] += total_keys


    # OUTPUT

    print("\n=== MEDIA PER DATASET E QUERY ===\n")

    for dataset in allowed_datasets:
        print("Dataset:", dataset)

        for query in allowed_queries:
            c = dataset_query_count[dataset][query]

            if c == 0:
                continue

            s = dataset_query_sum[dataset][query]
            print("  Query", query, ":", round(s/c,2))

        print()


    print("\n=== MEDIA GENERALE PER DATASET ===\n")

    for dataset in allowed_datasets:
        c = dataset_total_count[dataset]

        if c == 0:
            continue

        s = dataset_total_sum[dataset]
        print(dataset, ":",round(s/c,2))

def avg_pgkeymaker(folder_path,allowed_algorithms,allowed_datasets,allowed_queries):
    dataset_query_sum = {}
    dataset_query_count = {}

    dataset_total_sum = {}
    dataset_total_count = {}


    for dataset in allowed_datasets:

        dataset_query_sum[dataset] = {}
        dataset_query_count[dataset] = {}
        dataset_total_sum[dataset] = 0
        dataset_total_count[dataset] = 0

        for query in allowed_queries:

            dataset_query_sum[dataset][query] = 0
            dataset_query_count[dataset][query] = 0

            filename = "results_" + allowed_algorithms[0] + "_" + dataset + "_" + query + ".txt"
            file_path = os.path.join(folder_path, filename)

            if not os.path.exists(file_path):
                continue

            total_size = 0
            total_keys = 0

            with open(file_path, "r") as f:
                for line in f:
                    line = line.strip()

                    if ";" not in line:
                        continue

                    # 👉 parsing PGKeyMaker
                    keys_part = line.split(";", 1)[1].strip()

                    try:
                        keys = ast.literal_eval(keys_part)
                    except:
                        continue

                    if not isinstance(keys, (list, tuple)):
                        continue

                    for key in keys:
                        if isinstance(key, (list, tuple)):
                            total_size += len(key)
                            total_keys += 1
                        elif isinstance(key, str):
                            total_size += 1
                            total_keys += 1

            dataset_query_sum[dataset][query] += total_size
            dataset_query_count[dataset][query] += total_keys

            dataset_total_sum[dataset] += total_size
            dataset_total_count[dataset] += total_keys


    # OUTPUT

    print("\n=== MEDIA PER DATASET E QUERY ===\n")

    for dataset in allowed_datasets:
        print("Dataset:", dataset)

        for query in allowed_queries:
            c = dataset_query_count[dataset][query]

            if c == 0:
                continue

            s = dataset_query_sum[dataset][query]
            print("  Query", query, ":", round(s/c, 2))

        print()


    print("\n=== MEDIA GENERALE PER DATASET ===\n")

    for dataset in allowed_datasets:
        c = dataset_total_count[dataset]

        if c == 0:
            continue

        s = dataset_total_sum[dataset]
        print(dataset, ":", round(s/c, 2))

folder_path = "scalability/ucc_baseline1/stacked"

allowed_algorithms = ["HyUCC_bs1"]

allowed_datasets = [
    "synthea_100","synthea_200","synthea_300","synthea_400",
    "synthea_500","synthea_600","synthea_700","synthea_800",
    "synthea_900","synthea_1000","finbench","fraud"
]

allowed_queries = ["1","2","3","4","5","6","7","8"]

avg_baseline(folder_path,allowed_algorithms,allowed_datasets,allowed_queries)

folder_path = "scalability/pgkeymaker/stacked_results"

allowed_algorithms = ["PGKEYMAKER"]

allowed_datasets = [
    "synthea_100","synthea_200","synthea_300","synthea_400",
    "synthea_500","synthea_600","synthea_700","synthea_800",
    "synthea_900","synthea_1000","finbench","fraud"
]

allowed_queries = ["1","2","3","4","5","6","7","8"]

avg_pgkeymaker(folder_path,allowed_algorithms,allowed_datasets,allowed_queries)


