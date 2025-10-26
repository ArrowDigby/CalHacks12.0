#!/usr/bin/env python3
"""
Simple CSV to Parquet Converter
-------------------------------
Converts all CSV files in a directory to Parquet format.

Usage:
  python convert_csv_to_parquet.py --data-dir /path/to/csv/files --out-dir /path/to/output
"""

import duckdb
import time
import argparse
from pathlib import Path

def convert_csv_to_parquet(data_dir: Path, out_dir: Path):
    """Convert all CSV files to Parquet format, partitioned by event type."""
    
    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all CSV files
    csv_files = sorted(data_dir.glob("events_part_*.csv"))
    
    if not csv_files:
        print(f"❌ No events_part_*.csv files found in {data_dir}")
        return
    
    print(f"🟩 Found {len(csv_files)} CSV files to convert")
    print(f"📁 Input directory: {data_dir}")
    print(f"📁 Output directory: {out_dir}")
    print(f"📦 Strategy: Partitioned by event type")
    
    # Connect to DuckDB
    con = duckdb.connect()
    
    total_start = time.time()
    total_size_csv = 0
    
    # First pass: collect all data with input column
    print(f"\n🟦 Reading and combining all CSV files with input tracking...")
    read_start = time.time()
    
    # Build a UNION ALL query that adds the input filename as a column
    union_parts = []
    for csv_file in csv_files:
        # Extract the input identifier (e.g., "00000" from "events_part_00000.csv")
        input_id = csv_file.stem.replace("events_part_", "")
        total_size_csv += csv_file.stat().st_size / (1024 * 1024)
        
        union_parts.append(f"""
            SELECT 
                '{input_id}' AS input,
                *
            FROM read_csv('{csv_file.as_posix()}', 
                AUTO_DETECT = FALSE,
                HEADER = TRUE,
                COLUMNS = {{
                  'ts': 'VARCHAR',
                  'type': 'VARCHAR',
                  'auction_id': 'VARCHAR',
                  'advertiser_id': 'VARCHAR',
                  'publisher_id': 'VARCHAR',
                  'bid_price': 'VARCHAR',
                  'user_id': 'VARCHAR',
                  'total_price': 'VARCHAR',
                  'country': 'VARCHAR'
                }}
            )
        """)
    
    # Create a temporary view with all data
    union_query = " UNION ALL ".join(union_parts)
    con.execute(f"CREATE OR REPLACE VIEW all_events AS {union_query}")
    
    read_time = time.time() - read_start
    print(f"   ✅ All files read in {read_time:.1f}s")
    
    # Write partitioned parquet
    parquet_dir = out_dir / "events"
    print(f"\n🟦 Writing partitioned Parquet to {parquet_dir}...")
    print(f"   Partitioning by: type (groups events by type)")
    
    write_start = time.time()
    con.execute(f"""
        COPY (
            SELECT * FROM all_events
        ) TO '{parquet_dir.as_posix()}' (
            FORMAT 'parquet',
            PARTITION_BY (type),
            OVERWRITE_OR_IGNORE
        );
    """)
    write_time = time.time() - write_start
    
    print(f"   ✅ Parquet write completed in {write_time:.1f}s")
    
    # Calculate total parquet size
    total_size_parquet = sum(
        f.stat().st_size for f in parquet_dir.rglob("*.parquet")
    ) / (1024 * 1024)
    
    compression_ratio = (total_size_csv - total_size_parquet) / total_size_csv * 100
    
    total_time = time.time() - total_start
    
    con.close()
    
    print(f"\n" + "="*60)
    print(f"CONVERSION COMPLETE")
    print(f"="*60)
    print(f"📁 Files converted: {len(csv_files)}")
    print(f"📦 Partitions created: by event type")
    print(f"⏱️  Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"   - Read time: {read_time:.1f}s")
    print(f"   - Write time: {write_time:.1f}s")
    print(f"📊 Total size: {total_size_csv:.1f}MB → {total_size_parquet:.1f}MB ({compression_ratio:.1f}% reduction)")
    print(f"📁 Output directory: {parquet_dir}")
    print(f"\n💡 The data is now partitioned by 'type' column")
    print(f"   Each partition corresponds to a unique event type")

def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV files to Parquet format"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory containing CSV files"
    )
    parser.add_argument(
        "--out-dir", 
        type=Path,
        required=True,
        help="Directory to save Parquet files"
    )
    
    args = parser.parse_args()
    
    convert_csv_to_parquet(args.data_dir, args.out_dir)

if __name__ == "__main__":
    main()
