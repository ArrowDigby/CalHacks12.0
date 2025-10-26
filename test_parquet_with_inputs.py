#!/usr/bin/env python3
"""
Test Parquet Files with Input Queries
--------------------------------------
Runs the benchmark queries from inputs.py against Parquet files.

Usage:
  python test_parquet_with_inputs.py --parquet-dir /path/to/parquet/events
"""

import duckdb
import time
import argparse
from pathlib import Path
import csv
from assembler import assemble_sql
from inputs import queries


def run_queries_on_parquet(parquet_dir: Path, output_dir: Path = None):
    """Run benchmark queries from inputs.py on Parquet files."""
    
    parquet_path = (parquet_dir / "**/*.parquet").as_posix()
    
    print(f"📁 Reading from: {parquet_path}")
    print(f"📊 Running {len(queries)} queries from inputs.py\n")
    
    # Connect to DuckDB
    con = duckdb.connect()
    
    # Create view with derived columns (matching the CSV loading logic)
    print("🟩 Creating view with derived columns...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE VIEW events AS
        WITH raw AS (
            SELECT *
            FROM read_parquet('{parquet_path}')
        ),
        casted AS (
            SELECT
                to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0)    AS ts,
                type,
                auction_id,
                TRY_CAST(advertiser_id AS INTEGER)        AS advertiser_id,
                TRY_CAST(publisher_id  AS INTEGER)        AS publisher_id,
                NULLIF(bid_price, '')::DOUBLE             AS bid_price,
                TRY_CAST(user_id AS BIGINT)               AS user_id,
                NULLIF(total_price, '')::DOUBLE           AS total_price,
                country
            FROM raw
        )
        SELECT
            ts,
            DATE_TRUNC('week', ts)              AS week,
            DATE(ts)                            AS day,
            DATE_TRUNC('hour', ts)              AS hour,
            STRFTIME(ts, '%Y-%m-%d %H:%M')      AS minute,
            type,
            auction_id,
            advertiser_id,
            publisher_id,
            bid_price,
            user_id,
            total_price,
            country
        FROM casted;
    """)
    setup_time = time.time() - t0
    print(f"   ✅ View created in {setup_time:.3f}s\n")
    
    # Prepare output directory if specified
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run queries
    results = []
    total_query_time = 0
    
    for i, q in enumerate(queries, 1):
        print(f"🟦 Query {i}:")
        print(f"   {q}\n")
        
        # Convert JSON query to SQL
        sql = assemble_sql(q)
        print(f"   SQL: {sql}\n")
        
        # Execute query
        t0 = time.time()
        try:
            res = con.execute(sql)
            cols = [d[0] for d in res.description]
            rows = res.fetchall()
            dt = time.time() - t0
            total_query_time += dt
            
            print(f"   ✅ Rows: {len(rows)} | Time: {dt:.3f}s")
            
            # Show first few rows
            if len(rows) > 0:
                print(f"   Preview (first 5 rows):")
                for row in rows[:5]:
                    print(f"      {row}")
            print()
            
            # Save to CSV if output directory specified
            if output_dir:
                out_path = output_dir / f"q{i}.csv"
                with out_path.open("w", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(cols)
                    w.writerows(rows)
                print(f"   💾 Saved to: {out_path}\n")
            
            results.append({
                "query": i, 
                "rows": len(rows), 
                "time": dt,
                "status": "success"
            })
            
        except Exception as e:
            print(f"   ❌ Error: {e}\n")
            results.append({
                "query": i,
                "rows": 0,
                "time": 0,
                "status": f"error: {e}"
            })
    
    con.close()
    
    # Print summary
    print("=" * 70)
    print("QUERY SUMMARY")
    print("=" * 70)
    for r in results:
        status_icon = "✅" if r["status"] == "success" else "❌"
        print(f"{status_icon} Q{r['query']}: {r['time']:.3f}s ({r['rows']} rows) - {r['status']}")
    
    print(f"\n⏱️  Setup time: {setup_time:.3f}s")
    print(f"⏱️  Total query time: {total_query_time:.3f}s")
    print(f"⏱️  Total time: {setup_time + total_query_time:.3f}s")
    print("=" * 70)
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Test Parquet files with queries from inputs.py"
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        required=True,
        help="Directory containing Parquet files (the 'events' folder)"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Optional: Directory to save query results as CSV"
    )
    
    args = parser.parse_args()
    
    if not args.parquet_dir.exists():
        print(f"❌ Directory not found: {args.parquet_dir}")
        return
    
    run_queries_on_parquet(args.parquet_dir, args.out_dir)


if __name__ == "__main__":
    main()

