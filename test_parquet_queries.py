#!/usr/bin/env python3
"""
Test Queries on Parquet Files
------------------------------
Run queries against the partitioned Parquet files.

Usage:
  python test_parquet_queries.py --parquet-dir /path/to/parquet/events
"""

import duckdb
import time
import argparse
from pathlib import Path

def run_test_queries(parquet_dir: Path):
    """Run sample queries on the Parquet dataset."""
    
    parquet_path = (parquet_dir / "**/*.parquet").as_posix()
    
    print(f"📁 Reading from: {parquet_path}\n")
    
    # Connect to DuckDB
    con = duckdb.connect()
    
    # Test Query 1: Count by type
    print("🟦 Query 1: Count events by type")
    t0 = time.time()
    result = con.execute(f"""
        SELECT type, COUNT(*) as count
        FROM read_parquet('{parquet_path}')
        GROUP BY type
        ORDER BY type
    """).fetchall()
    dt = time.time() - t0
    
    print(f"   Results:")
    for row in result:
        print(f"     {row[0]}: {row[1]:,} events")
    print(f"   ✅ Time: {dt:.3f}s\n")
    
    # Test Query 2: Average bid price by type
    print("🟦 Query 2: Average bid price by type")
    t0 = time.time()
    result = con.execute(f"""
        SELECT 
            type, 
            AVG(TRY_CAST(bid_price AS DOUBLE)) as avg_bid,
            COUNT(*) as count
        FROM read_parquet('{parquet_path}')
        WHERE bid_price IS NOT NULL AND bid_price != ''
        GROUP BY type
        ORDER BY type
    """).fetchall()
    dt = time.time() - t0
    
    print(f"   Results:")
    for row in result:
        print(f"     {row[0]}: ${row[1]:.2f} avg (from {row[2]:,} events)")
    print(f"   ✅ Time: {dt:.3f}s\n")
    
    # Test Query 3: Filter by type (shows partition pruning benefit)
    print("🟦 Query 3: Count impressions only (partition pruning test)")
    t0 = time.time()
    result = con.execute(f"""
        SELECT COUNT(*) as impression_count
        FROM read_parquet('{parquet_path}')
        WHERE type = 'impression'
    """).fetchone()
    dt = time.time() - t0
    
    print(f"   Results: {result[0]:,} impressions")
    print(f"   ✅ Time: {dt:.3f}s (should be fast due to partition pruning)\n")
    
    # Test Query 4: Top countries
    print("🟦 Query 4: Top 10 countries by event count")
    t0 = time.time()
    result = con.execute(f"""
        SELECT country, COUNT(*) as count
        FROM read_parquet('{parquet_path}')
        GROUP BY country
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    dt = time.time() - t0
    
    print(f"   Results:")
    for i, row in enumerate(result, 1):
        print(f"     {i}. {row[0]}: {row[1]:,} events")
    print(f"   ✅ Time: {dt:.3f}s\n")
    
    con.close()
    
    print("=" * 60)
    print("✅ All test queries completed!")
    print("=" * 60)

def main():
    parser = argparse.ArgumentParser(
        description="Test queries on Parquet files"
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        required=True,
        help="Directory containing Parquet files (the 'events' folder)"
    )
    
    args = parser.parse_args()
    
    if not args.parquet_dir.exists():
        print(f"❌ Directory not found: {args.parquet_dir}")
        return
    
    run_test_queries(args.parquet_dir)

if __name__ == "__main__":
    main()

