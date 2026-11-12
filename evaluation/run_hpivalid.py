import pandas as pd
import subprocess
import re

PROJECT_DIR = "/mnt/c/Users/gianp/Desktop/HPIvalid/release"
INPUT_DIR = "/mnt/c/Users/gianp/Desktop/Codes/github/PGKeys-extractor/PG-Keys mining/evaluation"

def extract_runtime(output):
    match = re.search(r"run time:\s*([\d.]+)s", output)
    if match:
        return float(match.group(1))
    return None

def run_hpivalid(input_csv):
    cmd = [
        "wsl",
        "-d",
        "Ubuntu",
        "sh",
        "-c",
        f"cd '{PROJECT_DIR}' && ./HPIValid -h -i '{INPUT_DIR}/{input_csv}' -o '{INPUT_DIR}/ris_hpivalid'"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    return result.stdout


input_file = "example.csv"

output = run_hpivalid(input_file)
print(output)

runtime = extract_runtime(output)
print(runtime)