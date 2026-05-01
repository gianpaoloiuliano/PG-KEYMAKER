# PG-KEYMAKER — Property Graph Key Discovery
---

## 📖 Overview

**PG-KEYMAKER** is an algorithm for **PG Key discovery**: it automatically extracts **PG Keys** (sets of attributes that uniquely identify entities within a graph pattern) from a **Property Graph**, given a user-defined Cypher-like pattern.

The pipeline works as follows:

1. A **graph pattern** (e.g., `(p:Patient)-[:HAS_ENCOUNTER]->(e:Encounter)`) is decomposed into a hierarchy of **subpatterns**.
2. Each subpattern is **matched against the graph** via auto-generated Cypher queries.
3. For each match result, **UCC algorithms** (HyUCC or HPIValid) discover candidate keys.
4. Keys are **propagated and validated** bottom-up through the subpattern tree until keys for the full pattern are found.
5. When no internal keys exist, **external keys** (from neighboring nodes reachable) are discovered automatically.

---

## 📁 Project Structure

```
.
├── testing_optimized.py       # Entry point — orchestrates the full pipeline
├── subpattern_extractor.py    # Decomposes patterns into subpattern DAGs (NetworkX)
├── neo4j_querying_module.py   # Generates Cypher queries and executes them on Neo4j
├── PG_K_validator.py          # Key propagation and validation logic
├── metanome_ucc.py            # UCC computation (HyUCC via Metanome, or HPIValid via WSL)
└── temp_csvs/                 # Temporary CSVs used by HPIValid
```

---

## ⚙️ Requirements

- Python ≥ 3.8
- A running **Neo4j** instance (with APOC plugin for fast batch validation)
- **WSL / Ubuntu** (required only if using the `hpivalid` UCC algorithm)

### Python Dependencies

```bash
pip install neo4j networkx pandas matplotlib openclean-metanome psutil
```

> `openclean-metanome` requires a valid Metanome JAR configured via `openclean_metanome.config`.

---

## 🚀 Usage

### 1. Configure the Neo4j connection

In `testing_optimized.py`, set your database credentials:

```python
URI      = "bolt://localhost:7687"
USER     = "neo4j"
PASSWORD = "your_password"
DB_NAME  = "your_database"
```

### 2. Define your pattern

```python
pattern = "(p:Patient)-[e:HAS_ENCOUNTER]-(en:Encounter)"
```

### 3. Choose the UCC algorithm

| Value | Algorithm | Notes |
|---|---|---|
| `"hyucc"` | HyUCC (Metanome) | Cross-platform, Java-based |
| `"hpivalid"` | HPIValid | Requires WSL + Ubuntu |

### 4. Run

```python
python testing_optimized.py
```

Results are saved to `./evaluation/` as structured `.txt` files.

---

## 📊 Evaluation Output

Results are written to:

```
./evaluation/scalability/pgkeymaker_<alg>/stacked/
    results_PGKEYMAKER_<alg>_<db>_<test_id>.txt
```

Each file contains, for every pattern/subpattern node in the DAG:

```
pattern;keys found

(p:Patient)-[e:HAS_ENCOUNTER]-(en:Encounter) ; [['p.SSN'], ['p.name', 'p.birthdate']]
```

Execution times and memory usage are also logged per phase:

| Phase | Description |
|---|---|
| `ex_leaf` | UCC mining on leaf nodes |
| `ex_external` | External key search |
| `ex_case1/2/3` | Key propagation cases |

---

## 🔧 Configuration Notes

- If using `hpivalid`, update `PROJECT_DIR` and `CSV_DIR` in `metanome_ucc.py` to match your local paths.
- APOC must be enabled on your Neo4j instance for fast batch key checking (`check_key_uniqueness_apoc`).
- The `max_ucc_size` parameter in HyUCC controls the maximum key size (`-1` = unlimited).

---

## 📄 License

This project is intended for research use. See `LICENSE` for details.
