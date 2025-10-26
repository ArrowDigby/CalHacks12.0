#!/usr/bin/env python3
"""
Step 1: Prepare Data
--------------------
Reads data from CSV or Parquet files and creates:
1. Persisted table (events_persisted) in DuckDB
2. View pointing to Parquet files for fallback queries

Usage:
  # From CSV files (slow):
  python step1_prepare_data.py --data-dir ./data --out-dir ./out
  
  # From existing Parquet files (fast):
  python step1_prepare_data.py --parquet-dir /path/to/parquet/events --out-dir ./out
"""

import duckdb
import time
import argparse
from pathlib import Path
import os

# Configuration
DB_PATH = Path("tmp/baseline.duckdb")
TABLE_NAME = "events"
PERSISTED_TABLE = "events_persisted"


def load_data_from_csv(con, data_dir: Path):
    """Load CSV data into a view with proper typing and derived columns."""
    csv_files = list(data_dir.glob("events_part_*.csv"))

    if csv_files:
        print(f"🟩 Loading {len(csv_files)} CSV parts from {data_dir} ...")
        con.execute(f"""
            CREATE OR REPLACE VIEW {TABLE_NAME} AS
            WITH raw AS (
              SELECT *
              FROM read_csv(
                '{data_dir}/events_part_*.csv',
                AUTO_DETECT = FALSE,
                HEADER = TRUE,
                union_by_name = TRUE,
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
        print(f"🟩 Loading complete")
    else:
        raise FileNotFoundError(f"No events_part_*.csv found in {data_dir}")


def load_data_from_parquet(con, parquet_dir: Path):
    """Load Parquet data into a view with proper typing and derived columns."""
    parquet_path = (parquet_dir / "**/*.parquet").as_posix()
    
    print(f"🟩 Loading Parquet files from {parquet_dir} ...")
    con.execute(f"""
        CREATE OR REPLACE VIEW {TABLE_NAME} AS
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
    print(f"🟩 Loading complete")


def create_persisted_table(con):
    """Create a physical table from the events view."""
    print("🟩 Creating persisted table from events view...")
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE TABLE {PERSISTED_TABLE} AS SELECT * FROM {TABLE_NAME};")
    print(f"   ✓ Persisted table created in {time.time() - t0:.1f}s")


def write_partitioned_parquet(con, out_dir: Path):
    """Write partitioned Parquet files (type, day)."""
    parquet_root = out_dir / "parquet"
    parquet_events = parquet_root / "events"
    parquet_root.mkdir(parents=True, exist_ok=True)

    print("🟩 Writing partitioned Parquet (type, day) ...")
    print("   (This may take a few minutes for large datasets...)")
    t0 = time.time()
    con.execute(f"""
        COPY (
          SELECT * FROM {PERSISTED_TABLE}
          -- ORDER BY removed for faster streaming
        ) TO '{parquet_events.as_posix()}' (
          FORMAT 'parquet',
          PARTITION_BY (type, day),
          OVERWRITE_OR_IGNORE
        );
    """)
    print(f"   ✓ Parquet write completed in {time.time() - t0:.1f}s")


def create_parquet_view(con, parquet_dir: Path = None):
    """Create a view that points to Parquet files for fallback queries."""
    if parquet_dir:
        # Use external parquet directory (e.g., from convert_csv_to_parquet.py)
        parquet_path = (parquet_dir / "**/*.parquet").as_posix()
        print(f"🟩 Creating fallback view (using external Parquet: {parquet_dir})...")
        con.execute(f"""
            CREATE OR REPLACE VIEW events_parquet AS
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
            FROM (
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
                FROM read_parquet('{parquet_path}')
            );
        """)
        print("   ✓ View created (using external Parquet files)")
    else:
        # Use persisted table as fallback
        print("🟩 Creating fallback view (using persisted table)...")
        con.execute(f"""
            CREATE OR REPLACE VIEW events_parquet AS
            SELECT * FROM {PERSISTED_TABLE};
        """)
        print("   ✓ View created (using persisted table)")


def main():
    parser = argparse.ArgumentParser(
        description="Step 1: Prepare data - create persisted table and views"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="The folder where the input CSV files are located (required if not using --parquet-dir)"
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        help="Existing Parquet directory to load from (alternative to --data-dir)"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Where to output files (not used if loading from external Parquet)"
    )

    args = parser.parse_args()
    
    # Validate arguments
    if not args.data_dir and not args.parquet_dir:
        parser.error("Either --data-dir or --parquet-dir must be specified")
    if args.data_dir and args.parquet_dir:
        parser.error("Cannot specify both --data-dir and --parquet-dir")

    # Ensure directories exist
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(DB_PATH)
    
    # Tune DuckDB
    threads = os.cpu_count() or 4
    con.execute(f"PRAGMA threads={threads};")
    mem_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "14GB")
    con.execute(f"PRAGMA memory_limit='{mem_limit}';")
    con.execute("SET preserve_insertion_order=false;")

    print("=" * 60)
    print("STEP 1: PREPARE DATA")
    print("=" * 60)

    # Load data (from CSV or Parquet)
    if args.parquet_dir:
        if not args.parquet_dir.exists():
            print(f"❌ Error: Parquet directory not found: {args.parquet_dir}")
            return
        print(f"📦 Loading from external Parquet files (partitioned by type)")
        load_data_from_parquet(con, args.parquet_dir)
    else:
        print(f"📄 Loading from CSV files")
        load_data_from_csv(con, args.data_dir)
    
    # Create persisted table
    create_persisted_table(con)
    
    # Create fallback view
    if args.parquet_dir:
        # Use external parquet as fallback
        create_parquet_view(con, args.parquet_dir)
    else:
        # Use persisted table as fallback
        create_parquet_view(con, None)

    con.close()

    print("\n✅ Step 1 complete! Data is prepared and ready.")
    print(f"   Next: Run step2_build_rollups.py")


if __name__ == "__main__":
    main()

