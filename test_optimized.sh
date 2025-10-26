#!/bin/bash
set -e

echo "============================================"
echo "OPTIMIZED PARQUET TEST (DuckDB 1.1.3)"
echo "============================================"
echo ""

# Step 0: Verify DuckDB version
echo "Step 0: Checking DuckDB version..."
python -c "import duckdb; print(f'DuckDB version: {duckdb.__version__}')"
echo ""

# Clean old files
echo "Cleaning old files..."
rm -rf ./tmp ./output/events
echo ""

# Step 1: Generate optimized Parquet
echo "Step 1: Generating optimized Parquet files..."
python convert_csv_to_parquet.py \
  --data-dir /Users/matthewhu/Downloads/data \
  --out-dir ./output
echo ""

# Step 2: Load into database (skip rollups as requested)
echo "Step 2: Loading Parquet into database..."
python step1_prepare_data.py \
  --parquet-dir ./output/events \
  --out-dir ./output
echo ""

# Step 3: Run queries (NO ROLLUPS - direct Parquet access)
echo "Step 3: Running queries (NO ROLLUPS - testing pure Parquet performance)..."
python step3_run_queries.py \
  --out-dir ./output \
  --truth-dir ./query_results
echo ""

echo "============================================"
echo "TEST COMPLETE!"
echo "============================================"

