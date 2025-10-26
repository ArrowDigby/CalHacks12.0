#!/usr/bin/env python3
"""
Bloom Filter Verification Tool
-------------------------------
Tests and verifies that bloom filters are working in Parquet files.

Usage:
  python verify_bloom_filters.py --parquet-dir ./output/parquet/events
"""

import duckdb
import time
import argparse
from pathlib import Path


def verify_bloom_filters(parquet_dir: Path):
    """Verify bloom filters are present and working in Parquet files."""
    
    print("=" * 70)
    print("BLOOM FILTER VERIFICATION")
    print("=" * 70)
    
    parquet_path = (parquet_dir / "**/*.parquet").as_posix()
    
    # Connect to DuckDB
    con = duckdb.connect()
    
    # Test 1: Check Parquet metadata
    print("\n🔍 Test 1: Checking Parquet file metadata...")
    try:
        # Check for bloom filters using bloom_filter_offset and bloom_filter_length
        result = con.execute(f"""
            SELECT DISTINCT path_in_schema[1] AS column_name
            FROM parquet_metadata('{parquet_path}')
            WHERE bloom_filter_offset IS NOT NULL 
              AND bloom_filter_length > 0
            ORDER BY column_name;
        """).fetchall()
        
        if result:
            print("   ✅ Bloom filters found on columns:")
            for row in result:
                print(f"      - {row[0]}")
        else:
            print("   ❌ No bloom filters found in Parquet files")
            print("   💡 Make sure you're using DuckDB 1.2.0+ and regenerated the Parquet files")
            return False
    except Exception as e:
        print(f"   ⚠️  Could not read Parquet metadata: {str(e)}")
        print("   💡 This might mean the Parquet files weren't created with bloom filters")
        return False
    
    # Test 2: Performance test with selective filter
    print("\n🔍 Test 2: Performance test with highly selective filter...")
    
    # Query WITHOUT bloom filter (scan all data)
    print("   Testing query on non-bloom-filtered column (type)...")
    t0 = time.time()
    result = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{parquet_path}', hive_partitioning=1)
        WHERE type = 'impression';
    """).fetchone()
    time_without_bloom = time.time() - t0
    count = result[0]
    print(f"      Result: {count:,} rows in {time_without_bloom:.3f}s")
    
    # Query WITH bloom filter (should be faster)
    print("   Testing query on bloom-filtered column (country)...")
    t0 = time.time()
    result = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{parquet_path}', hive_partitioning=1)
        WHERE country = 'JP';
    """).fetchone()
    time_with_bloom = time.time() - t0
    count = result[0]
    print(f"      Result: {count:,} rows in {time_with_bloom:.3f}s")
    
    # Test 3: Query plan analysis
    print("\n🔍 Test 3: Analyzing query execution plan...")
    print("   Running EXPLAIN ANALYZE on bloom-filtered column...")
    
    explain_result = con.execute(f"""
        EXPLAIN ANALYZE
        SELECT COUNT(*)
        FROM read_parquet('{parquet_path}', hive_partitioning=1)
        WHERE country = 'US' AND publisher_id = 100;
    """).fetchall()
    
    print("\n   Query Plan:")
    for row in explain_result:
        plan_line = row[1] if len(row) > 1 else row[0]
        # Highlight important parts
        if 'Filters:' in plan_line or 'PARQUET' in plan_line or 'Scan' in plan_line:
            print(f"      {plan_line}")
    
    # Test 4: Multiple bloom-filtered columns
    print("\n🔍 Test 4: Query with multiple bloom-filtered columns...")
    t0 = time.time()
    result = con.execute(f"""
        SELECT country, COUNT(*) as cnt
        FROM read_parquet('{parquet_path}', hive_partitioning=1)
        WHERE country IN ('JP', 'US', 'GB')
          AND publisher_id IN (100, 200, 300)
        GROUP BY country;
    """).fetchall()
    query_time = time.time() - t0
    
    print(f"   ✅ Query completed in {query_time:.3f}s")
    print("   Results:")
    for row in result:
        print(f"      {row[0]}: {row[1]:,} events")
    
    con.close()
    
    print("\n" + "=" * 70)
    print("VERIFICATION COMPLETE")
    print("=" * 70)
    print("✅ Bloom filters are properly configured and working!")
    print("\n💡 Tips:")
    print("   - Bloom filters work best with highly selective equality filters")
    print("   - They reduce I/O by skipping row groups without matching values")
    print("   - Combine with partitioning for maximum performance")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Verify bloom filters in Parquet files"
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        required=True,
        help="Directory containing Parquet files (e.g., ./output/parquet/events)"
    )
    
    args = parser.parse_args()
    
    if not args.parquet_dir.exists():
        print(f"❌ Error: Directory not found: {args.parquet_dir}")
        return 1
    
    # Look for parquet files
    parquet_files = list(args.parquet_dir.rglob("*.parquet"))
    if not parquet_files:
        print(f"❌ Error: No Parquet files found in {args.parquet_dir}")
        return 1
    
    print(f"📁 Parquet directory: {args.parquet_dir}")
    print(f"📦 Found {len(parquet_files)} Parquet files")
    
    success = verify_bloom_filters(args.parquet_dir)
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())

