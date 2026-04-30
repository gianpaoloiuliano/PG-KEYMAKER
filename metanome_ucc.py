import openclean_metanome.config as config
from openclean_metanome.algorithm.hyucc import hyucc
import sys
import os
import subprocess


PROJECT_DIR = "/mnt/c/Users/gianp/Desktop/HPIvalid/release"
CSV_DIR = "/mnt/c/Users/gianp/Desktop/Codes/github/PGKeys-extractor/PG-Keys mining/temp_csvs"

# Forza Python a usare UTF-8 per stdin/stdout/stderr
os.environ["PYTHONIOENCODING"] = "utf-8"

# Se vuoi anche cambiare la code page della console corrente:
if sys.platform.startswith("win"):
    os.system("chcp 65001 > nul")
#download_jar(verbose=True)

def execute_hpivalid(csv_name):

    cmd = [
        "wsl",
        "-d",
        "Ubuntu",
        "sh",
        "-c",
        f"cd '{PROJECT_DIR}' && ./HPIValid -h -i '{CSV_DIR}/{csv_name}' -o '{CSV_DIR}'"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

def parse_hg_file(filepath, columns):
    uccs = []

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            indices = [
                int(x) for x in line.split(",")
                if x.strip() != ""
            ]

            ucc = [columns[i] for i in indices]
            uccs.append(ucc)

    return uccs

def compute_keys(df,ucc_alg):
    if ucc_alg == "hyucc":
        env = {config.METANOME_JARPATH: config.JARFILE()}
        keys = hyucc(df, env=env, verbose=False, max_ucc_size=-1)
        #print(keys)
        return keys
    elif ucc_alg == "hpivalid":
        columns = df.columns.tolist()
        rows, cols = df.shape
        df.to_csv("./temp_csvs/tmp.csv", index=False)
        execute_hpivalid("tmp.csv")
        hg_path = f"./temp_csvs/tmp_r{rows+1}_c{cols}_UCCs.hg"
        #hg_path = os.path.join("./temp_csvs", hg_filename)
        keys = parse_hg_file(hg_path, columns)
        print("uccs:", keys)
        return keys





#df2 = pd.read_csv("iris.csv")
#env = {config.METANOME_JARPATH: config.JARFILE()}
#keys = hyucc(df2, env=env)
#print(keys)