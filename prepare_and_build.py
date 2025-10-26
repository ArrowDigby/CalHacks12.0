#!/usr/bin/env python3
"""
Combined Data Preparation and Rollup Building
----------------------------------------------
This script combines Step 1 (data preparation) and Step 2 (rollup building)
into a single workflow for convenience.

Creates:
1. Persisted table (events_persisted) in DuckDB
2. View (events_raw) pointing to the persisted table
3. All 11 rollup tables for optimized query performance

Rollups created:
- by_day (day, type)
- by_country_day (day, country, type)
- by_publisher_day (day, publisher_id, type)
- by_advertiser_day (day, advertiser_id, type)
- by_publisher_country_day (day, publisher_id, country, type)
- by_advertiser_country_day (day, advertiser_id, country, type)
- by_publisher_advertiser_day (day, publisher_id, advertiser_id, type)
- by_minute (minute, day, type)
- by_country (country, type)
- by_publisher (publisher_id, type)
- by_advertiser (advertiser_id, type)

Usage:
  python prepare_and_build.py --data-dir ./data
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

# Rollup table names
ROLLUP_BY_DAY = "by_day"
ROLLUP_BY_COUNTRY_DAY = "by_country_day"
ROLLUP_BY_PUBLISHER_DAY = "by_publisher_day"
ROLLUP_BY_ADVERTISER_DAY = "by_advertiser_day"
ROLLUP_BY_PUBLISHER_COUNTRY_DAY = "by_publisher_country_day"
ROLLUP_BY_ADVERTISER_COUNTRY_DAY = "by_advertiser_country_day"
ROLLUP_BY_PUBLISHER_ADVERTISER_DAY = "by_publisher_advertiser_day"
ROLLUP_BY_MINUTE = "by_minute"
ROLLUP_BY_COUNTRY = "by_country"
ROLLUP_BY_PUBLISHER = "by_publisher"
ROLLUP_BY_ADVERTISER = "by_advertiser"


# ============================================================
# STEP 1: DATA PREPARATION
# ============================================================

def load_data(con, data_dir: Path):
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


def create_persisted_table(con):
    """Create a physical table from the events view."""
    print("🟩 Creating persisted table from events view...")
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE TABLE {PERSISTED_TABLE} AS SELECT * FROM {TABLE_NAME};")
    print(f"   ✓ Persisted table created in {time.time() - t0:.1f}s")


def create_fallback_view(con):
    """Create a view that points to the persisted table for queries that don't use rollups."""
    print("🟩 Creating fallback view (events_raw → events_persisted)...")
    con.execute(f"""
        CREATE OR REPLACE VIEW events_raw AS
        SELECT * FROM {PERSISTED_TABLE};
    """)
    print("   ✓ View created")


# ============================================================
# STEP 2: BUILD ROLLUPS
# ============================================================

def build_day_rollups(con):
    """Build rollups with day dimension."""
    print("🟩 Building day-level rollups...")
    
    print("   Building by_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_DAY} AS
        SELECT day, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, type;
    """)
    print(f"   ✓ by_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_country_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_COUNTRY_DAY} AS
        SELECT day, country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, country, type;
    """)
    print(f"   ✓ by_country_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_publisher_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_DAY} AS
        SELECT day, publisher_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, publisher_id, type;
    """)
    print(f"   ✓ by_publisher_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_advertiser_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER_DAY} AS
        SELECT day, advertiser_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, advertiser_id, type;
    """)
    print(f"   ✓ by_advertiser_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_publisher_country_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_COUNTRY_DAY} AS
        SELECT day, publisher_id, country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, publisher_id, country, type;
    """)
    print(f"   ✓ by_publisher_country_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_advertiser_country_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER_COUNTRY_DAY} AS
        SELECT day, advertiser_id, country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, advertiser_id, country, type;
    """)
    print(f"   ✓ by_advertiser_country_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_publisher_advertiser_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_ADVERTISER_DAY} AS
        SELECT day, publisher_id, advertiser_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, publisher_id, advertiser_id, type;
    """)
    print(f"   ✓ by_publisher_advertiser_day created in {time.time() - t0:.1f}s")


def build_minute_rollups(con):
    """Build minute-level rollups for fine-grained time analysis."""
    print("🟩 Building minute-level rollups...")
    
    print("   Building by_minute...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_MINUTE} AS
        SELECT minute, day, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY minute, day, type;
    """)
    print(f"   ✓ by_minute created in {time.time() - t0:.1f}s")


def build_dimension_rollups(con):
    """Build dimension-only rollups (no day)."""
    print("🟩 Building dimension-only rollups (no day) ...")
    
    print("   Building by_country...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_COUNTRY} AS
        SELECT country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY country, type;
    """)
    print(f"   ✓ by_country created in {time.time() - t0:.1f}s")
    
    print("   Building by_publisher...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER} AS
        SELECT publisher_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY publisher_id, type;
    """)
    print(f"   ✓ by_publisher created in {time.time() - t0:.1f}s")
    
    print("   Building by_advertiser...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER} AS
        SELECT advertiser_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY advertiser_id, type;
    """)
    print(f"   ✓ by_advertiser created in {time.time() - t0:.1f}s")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Combined data preparation and rollup building"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="The folder where the input CSV files are located"
    )

    args = parser.parse_args()

    # Ensure database directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(DB_PATH)
    
    # Tune DuckDB for optimal performance
    threads = os.cpu_count() or 4
    con.execute(f"PRAGMA threads={threads};")
    mem_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "14GB")
    con.execute(f"PRAGMA memory_limit='{mem_limit}';")
    con.execute("SET preserve_insertion_order=false;")

    print("=" * 60)
    print("DATA PREPARATION & ROLLUP BUILDING")
    print("=" * 60)
    print(f"📊 Using {threads} threads and {mem_limit} memory limit")
    
    overall_start = time.time()

    # ============================================================
    # STEP 1: PREPARE DATA
    # ============================================================
    print("\n" + "=" * 60)
    print("STEP 1: PREPARE DATA")
    print("=" * 60)
    
    step1_start = time.time()
    load_data(con, args.data_dir)
    create_persisted_table(con)
    create_fallback_view(con)
    step1_time = time.time() - step1_start
    
    print(f"\n✅ Step 1 complete in {step1_time:.1f}s")

    # ============================================================
    # STEP 2: BUILD ROLLUPS
    # ============================================================
    print("\n" + "=" * 60)
    print("STEP 2: BUILD ROLLUPS")
    print("=" * 60)
    
    step2_start = time.time()
    build_day_rollups(con)
    build_minute_rollups(con)
    build_dimension_rollups(con)
    step2_time = time.time() - step2_start
    
    print(f"\n✅ Step 2 complete in {step2_time:.1f}s")

    con.close()

    # ============================================================
    # SUMMARY
    # ============================================================
    total_time = time.time() - overall_start
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Step 1 (Data Preparation): {step1_time:.1f}s")
    print(f"Step 2 (Rollup Building):  {step2_time:.1f}s")
    print(f"Total Time:                {total_time:.1f}s")
    print("=" * 60)
    print(f"\n✅ All done! Database ready at {DB_PATH}")
    print(f"   Next: Run step3_run_queries.py to execute queries")


if __name__ == "__main__":
    main()

