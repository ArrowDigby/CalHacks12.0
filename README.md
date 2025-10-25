# Optimized Query Engine

A high-performance query system using DuckDB with rollups and partitioned data

## Installation

```bash
pip install -r requirements.txt
```

Requirements:
- `duckdb>=1.1.1`
- `pandas>=2.2.0`

## Quick Start

### Option 1: All-in-One (Simple)
```bash
python main.py --data-dir ./data-lite --out-dir ./output-lite
```

Runs all 3 steps in sequence: data prep, rollup building, and query execution.

### Option 2: Modular Pipeline (Recommended)
```bash
# Step 1: Prepare data
python step1_prepare_data.py --data-dir ./data-lite --out-dir ./output-lite --skip-parquet

# Step 2: Build rollups
python step2_build_rollups.py

# Step 3: Run queries
python step3_run_queries.py
```

Run steps independently for faster iteration and development.

## Scripts

### `step1_prepare_data.py` - Data Preparation

**What it does:**
- Loads CSV files into DuckDB
- Creates a persisted table (`events_persisted`)
- Optionally writes partitioned Parquet files

**Arguments:**
- `--data-dir PATH` - Directory with CSV files (required)
- `--out-dir PATH` - Output directory (required)
- `--skip-parquet` - Skip Parquet writing for faster preprocessing (recommended)

### `step2_build_rollups.py` - Build Rollups

**What it does:**
- Creates 7 pre-aggregated rollup tables for fast query execution

**Arguments:** None

### `step3_run_queries.py` - Run Queries

**What it does:**
- Executes benchmark queries from `inputs.py`
- Automatically routes to optimal data source (rollup vs raw data)
- Caches results for repeated queries
- Shows performance metrics in terminal

**Arguments:** None

**Note:** No CSV files are written - output is terminal only.

## Usage Examples

### Recommended Workflow
```bash
# One-time setup
python step1_prepare_data.py --data-dir ./data --out-dir ./output --skip-parquet
python step2_build_rollups.py

# Run queries (repeat as needed)
python step3_run_queries.py
```

### With Parquet Files
```bash
# Include Parquet (slower preprocessing, but enables partition pruning)
python step1_prepare_data.py --data-dir ./data --out-dir ./output
python step2_build_rollups.py
python step3_run_queries.py
```

### All-in-One
```bash
# Simple single command (runs all 3 steps)
python main.py --data-dir ./data-lite --out-dir ./output-lite
```

## Maintenance

### Start Fresh
Remove database and Parquet files:
```bash
rm -rf tmp/baseline.duckdb* output-lite/parquet/
```

### View Database Contents
```bash
# Using DuckDB CLI
duckdb tmp/baseline.duckdb

# Inside DuckDB:
SHOW TABLES;
SELECT COUNT(*) FROM by_day;
SELECT * FROM by_country LIMIT 10;
```

## Troubleshooting

### Out of Memory
```bash
export DUCKDB_MEMORY_LIMIT=12GB
python step1_prepare_data.py ...
```

### Database Locked
Another process is using the database. Close it or:
```bash
pkill -9 -f "python.*main.py"
rm tmp/baseline.duckdb.wal
```

### Slow Preprocessing
Use `--skip-parquet` flag to skip Parquet writing for faster preprocessing.

## File Structure
```
CalHacks12.0/
├── main.py                    # All-in-one script
├── step1_prepare_data.py      # Data preparation
├── step2_build_rollups.py     # Rollup generation
├── step3_run_queries.py       # Query execution
├── assembler.py               # SQL query builder
├── inputs.py                  # Query definitions
├── requirements.txt           # Python dependencies
├── tmp/
│   └── baseline.duckdb        # DuckDB database file
└── output-lite/
    └── parquet/events/        # Optional Parquet files
```