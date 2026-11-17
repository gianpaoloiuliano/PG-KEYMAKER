# PG-KEYMAKER — Property Graph Key Discovery
---

## 📖 Overview

**PG-KEYMAKER** is an algorithm for **PG Key discovery**: it automatically extracts **PG Keys** (sets of attributes that uniquely identify entities within a graph pattern) from a **Property Graph**, given a user-defined Cypher-like pattern, optionally restricted by **filters**.

The pipeline works as follows:

1. A **graph pattern** (e.g., `(p:Patient)-[:HAS_ENCOUNTER]->(e:Encounter)`) is decomposed into a hierarchy of **subpatterns** (the SP-Tree, a NetworkX DAG).
2. Each subpattern is **matched against the graph** via auto-generated Cypher queries (filters, if any, are injected in the match block).
3. For each match result, **UCC algorithms** (HyUCC or HPIValid) discover candidate keys.
4. Keys are **propagated and validated** bottom-up through the subpattern tree until keys for the full pattern are found.
5. When no internal keys exist, **external keys** (from neighboring nodes reachable) are discovered automatically.

A **notree** variant (no subpattern tree reuse) is also available and is used as ablation in the evaluation.

## 📄 Datasets

You can download the datasets used for the evaluation at this link: https://zenodo.org/records/24310924?preview=1&token=eyJhbGciOiJIUzUxMiJ9.eyJpZCI6Ijc0NThhYjliLTYyZGItNDdlYS1hYmQwLWM1MzY4NmNhMmM3ZCIsImRhdGEiOnt9LCJyYW5kb20iOiJiMGYxMWUxZWM2ZGQyNmQ3YWM5ODI0ZDFmNTU5Zjk0MSJ9.p9_S7-2Rm9Z-BxAJS-ZzpOHxJJ_BVYQazVDsZCTPEhrxQO5Uabbm48UYwbLbLCjdsL6wjT3ERHjnT8ClMS0rzQ

---

## 📁 Project Structure

```
.
├── testing_optimized.py          # Entry point — pipeline with pattern/filters defined in the script
├── testing_optimized_wargs.py    # Same pipeline, driven by a command-line interface (argparse)
├── run_synthea_experiments.py    # Batch runner for the Synthea experiments (3/4/5 hops, tree/notree, hyucc/hpivalid)
├── subpattern_extractor_v2.py    # Decomposes patterns into subpattern DAGs (NetworkX)
├── neo4j_querying_module.py      # Generates Cypher queries and executes them on Neo4j
├── query_filters.py              # Parsing and application of simple/aggregate filters
├── PG_K_validator.py             # Key propagation and validation logic
├── metanome_ucc.py               # UCC computation (HyUCC via Metanome, or HPIValid via WSL)
├── data/                         # Neo4j 5.26.1 database dumps (zip, tracked with Git LFS)
├── patterns/                     # Synthea patterns (synthea_{3,4,5}hops.json) used by run_synthea_experiments.py
├── use_case/                     # Noise / integrity-checking experiment on FinBench
│   ├── noise_experiment.py       #   sub-commands: validate, compare
│   └── details_*.csv, summary_*.json
└── evaluation/
    ├── baselines/                # HyUCC, HPIValid, GED and GFD-adapted baselines (HPIValid = executable)
    ├── scalability/              # Raw result files (see "Evaluation Output")
    ├── csv/                      # Aggregated results
    ├── plots/                    # Generated figures (PDF)
    ├── results_and_plots_v2.py   # Aggregation and plotting
    ├── compare_best_times.py     # Best-version-per-query comparison
    ├── avg_keys_size.py, stats.py
    └── run_hpivalid.py           # HPIValid runner (WSL)
```

---

## ⚙️ Requirements

- Python ≥ 3.8
- A running **Neo4j** instance (with APOC plugin for fast batch validation). All datasets are loaded with Neo4j version **5.26.1**.
- **Git LFS** to download the dumps in `data/` (`git lfs install && git lfs pull`)
- **WSL / Ubuntu** (required only if using the `hpivalid` UCC algorithm)

### Python Dependencies

```bash
pip install neo4j networkx pandas matplotlib openclean-metanome psutil
```

> `openclean-metanome` requires a valid Metanome JAR configured via `openclean_metanome.config`.

### Datasets

`data/` contains the dumps used in the experiments: `Synthea_100` … `Synthea_1000`, `Fraud`, `finbench` and `network` (files named `<name>__5_26_1.zip`). Unzip a dump and load it into a Neo4j 5.26.1 database before running the algorithm.

---

## 🚀 Usage

### Option A — Edit `testing_optimized.py`

1. Set the Neo4j connection:

```python
URI  = "neo4j://localhost:7687"
AUTH = ("neo4j", "your_password")
```

2. Set the dataset name, the test number and the UCC algorithm (`nomedb`, `numtest`, `ucc_alg`, `tree`).
3. Define the patterns as `(pattern, filters)` pairs:

```python
patterns = [
    ("(p:Patient)-[e:HAS_ENCOUNTER]-(en:Encounter)", ""),
]
```

4. Run:

```bash
python testing_optimized.py
```

### Option B — Command line (`testing_optimized_wargs.py`)

```bash
python testing_optimized_wargs.py --db synthea_100 --numtest 1 --tree true --ucc-alg hyucc \
    --pattern "(p:Patient)-[e:HAS_ENCOUNTER]-(en:Encounter)"
```

| Argument | Description |
|---|---|
| `--db` | Dataset name (used in result file names) — required |
| `--numtest` | Number of the first test, incremented for each pattern — required |
| `--tree` | `true` = with SP-Tree (default), `false` = notree |
| `--ucc-alg` | `hyucc` (default) or `hpivalid` |
| `--patterns-file` | `.json` or `.txt` file with the patterns |
| `--pattern`, `--filters` | Inline pattern / its filters (repeatable, same order) |
| `--typology` | Results sub-folder (tree only, default `other_shapes`) |
| `--lenpattern` | Pattern length label (default `len_2`) |
| `--database` | Neo4j database to query (default: the default database) |
| `--skip-existing` | Skip tests whose result file is already complete (resume) |
| `--check-only` | Only check whether tests are pending, without touching Neo4j |
| `--uri`, `--user`, `--password` | Neo4j connection (default `neo4j://localhost:7687`, user `neo4j`) |

### Option C — Batch experiments

```bash
python run_synthea_experiments.py
```

Runs, for each Synthea database, all combinations of tree/notree, `hyucc`/`hpivalid` and 3/4/5 hops (patterns read from `patterns/synthea_<hop>hops.json`). Existing complete results are skipped.

### Filters

Filters are a free Cypher string; top-level `AND`s split it into single filters:

```
p.age < 18 AND en1.cost IS NOT NULL AND avg(en1.cost) > 100 PER p
```

- **Simple filter**: a Cypher condition on the pattern aliases, added to the `WHERE` clause.
- **Aggregate filter** (`avg`, `min`, `max`, `sum`, `count`, …): must end with `PER <node_alias>`; the aggregate is computed per node on the minimal contiguous portion of the pattern containing the aliases involved.

A filter applies to a node of the SP-Tree only if all its aliases appear in it.

### Algorithm choice

| Value | Algorithm | Notes |
|---|---|---|
| `"hyucc"` | HyUCC (Metanome) | Cross-platform, Java-based |
| `"hpivalid"` | HPIValid | Requires WSL + Ubuntu |

---

## 📊 Evaluation Output

Results are written to:

```
./evaluation/scalability/pgkeymaker_<alg>/<typology>/
    results_PGKEYMAKER_<alg>_<db>_<test_id>.txt
./evaluation/scalability/ablation/pgkeymaker_<alg>_notree/
    results_PGKEYMAKER-NOTREE_<db>_<test_id>.txt
```

`<typology>` is one of `stacked`, `meaningfull`, `other_shapes`. Baseline results are in `evaluation/scalability/baseline_{hyucc,hpivalid,ged,gfd}/`.

Each file contains, for every pattern/subpattern node in the DAG:

```
pattern;keys found

(p:Patient)-[e:HAS_ENCOUNTER]-(en:Encounter) ; [['p.SSN'], ['p.name', 'p.birthdate']]
```

Execution times and memory usage are also logged per phase (a file is considered complete when its last line is `Max memory used`):

| Phase | Description |
|---|---|
| `ex_leaf` | UCC mining on leaf nodes |
| `ex_external` | External key search |
| `ex_case1/2/3` | Key propagation cases |

### Analysis and plots

Run from `evaluation/`: `results_and_plots_v2.py` (aggregation and figures in `plots/`), `compare_best_times.py` (best version per query, e.g. `--include-ablation`, `--csv out.csv`), `avg_keys_size.py`, `stats.py`.

### Noise / integrity use case

```bash
python use_case/noise_experiment.py validate ...   # re-validate clean-graph keys on the database
python use_case/noise_experiment.py compare ...    # compare keys on clean vs. noisy graph
```

Outputs (`details_finbench_<p>perc_*.csv`, `summary_finbench_<p>perc.json`) are written in `use_case/`. Use `--help` for the available options.

---

## 🔧 Configuration Notes

- If using `hpivalid`, update `PROJECT_DIR` and `CSV_DIR` in `metanome_ucc.py` (lines 8–9, WSL paths) to match your local setup; temporary CSVs are written to `CSV_DIR`.
- APOC must be enabled on your Neo4j instance for fast batch key checking (`check_key_uniqueness_apoc`).
- The `max_ucc_size` parameter in HyUCC controls the maximum key size (`-1` = unlimited).
- Neo4j credentials are set in the scripts / via CLI: replace the defaults with your own.

---

## 📄 License

This project is intended for research use.
