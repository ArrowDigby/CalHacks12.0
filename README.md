# Optimized Query Engine

A high-performance query system using DuckDB with rollups, partitioned data, and bloom filters

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

## Performance Optimizations

### Bloom Filters (Parquet Branch)

This implementation uses **bloom filters** in Parquet files to accelerate queries with equality filters. Bloom filters enable DuckDB to skip entire row groups that don't contain the filtered values.

**Quick Start with Bloom Filters:**
```bash
# Method 1: Convert existing CSV to Parquet with bloom filters
python convert_csv_to_parquet.py \
  --data-dir /Users/matthewhu/Downloads/data \
  --out-dir ./output

# Method 2: Use step1 to create Parquet files with bloom filters
python step1_prepare_data.py \
  --data-dir /Users/matthewhu/Downloads/data \
  --out-dir ./output

# Verify bloom filters are working
python verify_bloom_filters.py --parquet-dir ./output/events
```

**How it works:**
- Bloom filters are **automatically** applied by DuckDB 1.2.0+ to columns with dictionary encoding
- Columns with high cardinality (like `country`, `publisher_id`, `advertiser_id`, `auction_id`) get bloom filters
- No manual column selection needed - DuckDB decides based on data characteristics

**Benefits:**
- 10-100x speedup for highly selective filters
- Reduces I/O by skipping irrelevant row groups
- Minimal storage overhead (~1-2% of file size)
- Works automatically - no configuration required

**Configuration:**
- Row group size: 122,880 rows (DuckDB default, optimal balance)
- Compression: SNAPPY (faster reads than ZSTD, slight size increase)
- Partitioning: By `type` (and `day` in some cases)
- **Requires:** DuckDB 1.2.0 or later
- Bloom filters: Automatic on high-cardinality columns only

Bloom filters are automatically created during Parquet file generation in:
- `convert_csv_to_parquet.py` - Initial CSV to Parquet conversion
- `step1_prepare_data.py` - When writing partitioned Parquet files

**Verifying Bloom Filters:**

Run the verification script to test bloom filter functionality:
```bash
python verify_bloom_filters.py --parquet-dir ./output/parquet/events
```

This script will:
1. Check Parquet metadata for bloom filter presence
2. Run performance tests comparing bloom-filtered vs non-filtered queries
3. Analyze query execution plans
4. Test multi-column bloom filter queries

You can also manually verify using DuckDB:
```python
import duckdb
con = duckdb.connect()

# Check which columns have bloom filters
con.execute("""
    SELECT DISTINCT path_in_schema[1] AS column_name, has_bloom_filter
    FROM parquet_metadata('output/parquet/events/**/*.parquet')
    WHERE has_bloom_filter = true
""").fetchall()
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
├── convert_csv_to_parquet.py  # CSV to Parquet converter (with bloom filters)
├── verify_bloom_filters.py    # Bloom filter verification tool
├── requirements.txt           # Python dependencies
├── tmp/
│   └── baseline.duckdb        # DuckDB database file
└── output/
    └── parquet/events/        # Parquet files with bloom filters
```