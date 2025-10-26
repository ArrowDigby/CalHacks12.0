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
    """Convert all CSV files to Parquet format."""
    
    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all CSV files
    csv_files = list(data_dir.glob("events_part_*.csv"))
    
    if not csv_files:
        print(f"❌ No events_part_*.csv files found in {data_dir}")
        return
    
    print(f"🟩 Found {len(csv_files)} CSV files to convert")
    print(f"📁 Input directory: {data_dir}")
    print(f"📁 Output directory: {out_dir}")
    
    # Connect to DuckDB
    con = duckdb.connect()
    
    total_start = time.time()
    total_size_mb = 0
    
    for i, csv_file in enumerate(csv_files, 1):
        print(f"\n🟦 Converting file {i}/{len(csv_files)}: {csv_file.name}")
        
        # Create corresponding parquet filename
        parquet_file = out_dir / f"{csv_file.stem}.parquet"
        
        # Get file size for reporting
        csv_size_mb = csv_file.stat().st_size / (1024 * 1024)
        total_size_mb += csv_size_mb
        
        # Convert CSV to Parquet
        start_time = time.time()
        
        con.execute(f"""
            COPY (
              SELECT *
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
            ) TO '{parquet_file.as_posix()}' (
              FORMAT 'parquet',
              OVERWRITE_OR_IGNORE
            );
        """)
        
        conversion_time = time.time() - start_time
        
        # Get parquet file size
        parquet_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        compression_ratio = (csv_size_mb - parquet_size_mb) / csv_size_mb * 100
        
        print(f"   ✅ Converted in {conversion_time:.1f}s")
        print(f"   📊 Size: {csv_size_mb:.1f}MB → {parquet_size_mb:.1f}MB ({compression_ratio:.1f}% reduction)")
    
    total_time = time.time() - total_start
    
    con.close()
    
    print(f"\n" + "="*60)
    print(f"CONVERSION COMPLETE")
    print(f"="*60)
    print(f"📁 Files converted: {len(csv_files)}")
    print(f"⏱️  Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"📊 Total data: {total_size_mb:.1f}MB")
    print(f"⚡ Average per file: {total_time/len(csv_files):.1f}s")
    print(f"📁 Output directory: {out_dir}")

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
