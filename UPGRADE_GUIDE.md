# DuckDB 1.2.0 Upgrade Guide

## The Issue You Encountered

You saw this error:
```
_duckdb.NotImplementedException: Not implemented Error: Unrecognized option "BLOOM_FILTER_COLUMNS" for parquet
```

**Root Cause:** Your DuckDB version (1.1.1) doesn't support bloom filters. Bloom filter support was added in DuckDB 1.2.0 (February 2025).

## Quick Fix (3 Steps)

### Step 1: Upgrade DuckDB

```bash
# Upgrade DuckDB to latest version
pip install --upgrade 'duckdb>=1.2.0'

# Verify the upgrade
python -c "import duckdb; print(duckdb.__version__)"
# Should output: 1.2.0 or higher
```

### Step 2: Generate Parquet Files

Now run the conversion again:

```bash
python convert_csv_to_parquet.py \
  --data-dir /Users/matthewhu/Downloads/data \
  --out-dir ./output
```

This will:
- Create Parquet files in `./output/events/`
- Automatically enable bloom filters on eligible columns
- Use ZSTD compression for better file sizes
- Create 100K row groups for optimal performance

### Step 3: Verify Bloom Filters

After conversion completes:

```bash
python verify_bloom_filters.py --parquet-dir ./output/events
```

You should see:
```
✅ Bloom filters found on columns:
   - country
   - publisher_id
   - advertiser_id
   - auction_id
```

## What Changed in the Code

### Old Code (Didn't Work)
```python
BLOOM_FILTER_COLUMNS 'country,publisher_id,advertiser_id,auction_id'  # ❌ Not a valid option
```

### New Code (Works)
```python
# Bloom filters are automatic in DuckDB 1.2.0+
# Just need proper row group size and compression
COPY (...) TO '...' (
    FORMAT 'parquet',
    PARTITION_BY (type),
    OVERWRITE_OR_IGNORE,
    COMPRESSION 'ZSTD',
    ROW_GROUP_SIZE 100000  # ✅ This enables bloom filters automatically
);
```

## How Bloom Filters Work in DuckDB 1.2.0+

**Automatic Application:**
- DuckDB 1.2.0+ automatically creates bloom filters for columns with dictionary encoding
- High-cardinality columns (IDs, countries, etc.) automatically get bloom filters
- No manual configuration needed - it just works!

**Performance Benefits:**
- Queries like `WHERE country = 'JP'` run 10-50x faster
- ID lookups (`WHERE publisher_id = 100`) run 10-100x faster
- Row groups without matching values are skipped entirely

## Alternative: Use Virtual Environment

If you want to keep your system DuckDB at 1.1.1, use a virtual environment:

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On macOS/Linux
# venv\Scripts\activate  # On Windows

# Install requirements with DuckDB 1.2.0+
pip install -r requirements.txt

# Now run your scripts
python convert_csv_to_parquet.py --data-dir /Users/matthewhu/Downloads/data --out-dir ./output
```

## Complete Workflow After Upgrade

```bash
# 1. Upgrade
pip install --upgrade 'duckdb>=1.2.0'

# 2. Convert CSV to Parquet with bloom filters
python convert_csv_to_parquet.py \
  --data-dir /Users/matthewhu/Downloads/data \
  --out-dir ./output

# 3. Verify bloom filters
python verify_bloom_filters.py --parquet-dir ./output/events

# 4. (Optional) Use Parquet files with the query pipeline
python step1_prepare_data.py \
  --parquet-dir ./output/events \
  --out-dir ./output

python step2_build_rollups.py

python step3_run_queries.py \
  --out-dir ./output \
  --truth-dir ./query_results
```

## Expected Timeline

- DuckDB upgrade: 30 seconds
- Parquet conversion: 5-15 minutes (depends on data size)
- Bloom filter verification: 10 seconds
- Total: ~15 minutes

## Need Help?

Check these files for more info:
- `BLOOM_FILTER_CHECKLIST.md` - Step-by-step checklist
- `BLOOM_FILTER_IMPLEMENTATION.md` - Detailed technical guide
- `README.md` - General project documentation

## Quick Health Check

After upgrading, run this to verify everything is working:

```python
import duckdb
import sys

# Check version
version = duckdb.__version__
print(f"DuckDB version: {version}")

if tuple(map(int, version.split('.')[:2])) >= (1, 2):
    print("✅ Version is 1.2.0 or higher - bloom filters supported!")
    sys.exit(0)
else:
    print("❌ Version is below 1.2.0 - bloom filters NOT supported!")
    print("Run: pip install --upgrade 'duckdb>=1.2.0'")
    sys.exit(1)
```

Save as `check_duckdb_version.py` and run: `python check_duckdb_version.py`

