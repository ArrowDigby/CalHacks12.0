# Query Engine Benchmark

## Setup

Install dependencies:
```bash
pip install -r requirements.txt
```

Requirements: `duckdb>=1.1.1`, `pandas>=2.2.0`

## Adding Your Queries

Add your benchmark queries to `inputs.py` using the provided format:
```python
queries = [
    {
        "select": ["day", {"SUM": "total_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "click"}],
        "group_by": ["day"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    # Add more queries here...
]
```

## Running the Benchmark

### Step 1: Prepare Data and Build Rollups
```bash
python prepare_and_build.py --data-dir /path/to/data
```

This will:
- Load CSV files into DuckDB
- Create optimized rollup tables
- Takes ~8-9 minutes on the full dataset (on an Apple M1)

### Step 2: Run Queries
```bash
python step3_run_queries.py --out-dir /path/to/output --truth-dir /path/to/truth
```

This will:
- Execute all benchmark queries
- Output results to CSV files
- Compare with ground truth (if provided)
- Display performance metrics

## Example

```bash
# Prepare data (one-time setup)
python prepare_and_build.py --data-dir ./data

# Run queries
python step3_run_queries.py --out-dir ./output --truth-dir ./truth
```

## File Structure

```
CalHacks12.0/
├── prepare_and_build.py   # Data preparation + rollup building (REQUIRED)
├── step3_run_queries.py   # Query execution (REQUIRED)
├── inputs.py              # Query definitions (ADD YOUR QUERIES HERE)
├── assembler.py           # SQL query builder
├── requirements.txt       # Dependencies
├── step1_prepare_data.py  # Optional: Run data prep separately
├── step2_build_rollups.py # Optional: Run rollup building separately
└── tmp/
    └── baseline.duckdb    # DuckDB database (created on first run)
```

## Optional: Modular Workflow

For development/debugging, you can run steps separately:
```bash
# Step 1 only: Prepare data
python step1_prepare_data.py --data-dir ./data

# Step 2 only: Build rollups (after step 1)
python step2_build_rollups.py

# Step 3: Run queries
python step3_run_queries.py --out-dir ./output --truth-dir ./truth
```

## Cleanup

To start fresh:
```bash
rm -rf tmp/baseline.duckdb*
```
