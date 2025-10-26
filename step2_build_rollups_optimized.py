#!/usr/bin/env python3
"""
Step 2: Build Rollups (Optimized)
---------------------------------
Enhanced version with indexes, parallel processing, and advanced rollups.

Key optimizations:
- Strategic indexes on all rollup tables
- Parallel rollup creation
- Additional rollup combinations
- Optimized column ordering
"""

import duckdb
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configuration
DB_PATH = Path("tmp/baseline.duckdb")
PERSISTED_TABLE = "events_persisted"

# Rollup table names
ROLLUP_BY_DAY = "by_day"
ROLLUP_BY_COUNTRY_DAY = "by_country_day"
ROLLUP_BY_PUBLISHER_DAY = "by_publisher_day"
ROLLUP_BY_ADVERTISER_DAY = "by_advertiser_day"
ROLLUP_BY_PUBLISHER_COUNTRY_DAY = "by_publisher_country_day"
ROLLUP_BY_MINUTE = "by_minute"
ROLLUP_BY_COUNTRY = "by_country"
ROLLUP_BY_PUBLISHER = "by_publisher"
ROLLUP_BY_ADVERTISER = "by_advertiser"

# Additional optimized rollups
ROLLUP_BY_HOUR = "by_hour"
ROLLUP_BY_WEEK = "by_week"
ROLLUP_BY_TYPE_ONLY = "by_type_only"


def create_indexes(con):
    """Create strategic indexes on all rollup tables."""
    print("🟩 Creating strategic indexes...")
    
    index_definitions = [
        # Day-level rollups
        (ROLLUP_BY_DAY, "day, type"),
        (ROLLUP_BY_COUNTRY_DAY, "country, day, type"),
        (ROLLUP_BY_PUBLISHER_DAY, "publisher_id, day, type"),
        (ROLLUP_BY_ADVERTISER_DAY, "advertiser_id, day, type"),
        (ROLLUP_BY_PUBLISHER_COUNTRY_DAY, "publisher_id, country, day, type"),
        
        # Minute-level rollups
        (ROLLUP_BY_MINUTE, "minute, day, type"),
        
        # Dimension-only rollups
        (ROLLUP_BY_COUNTRY, "country, type"),
        (ROLLUP_BY_PUBLISHER, "publisher_id, type"),
        (ROLLUP_BY_ADVERTISER, "advertiser_id, type"),
        
        # Additional rollups
        (ROLLUP_BY_HOUR, "hour, day, type"),
        (ROLLUP_BY_WEEK, "week, type"),
        (ROLLUP_BY_TYPE_ONLY, "type"),
    ]
    
    for table_name, columns in index_definitions:
        try:
            index_name = f"idx_{table_name}_{columns.replace(', ', '_')}"
            con.execute(f"""
                CREATE INDEX IF NOT EXISTS {index_name} 
                ON {table_name} ({columns});
            """)
            print(f"   ✓ Index created: {index_name}")
        except Exception as e:
            print(f"   ⚠️  Index failed for {table_name}: {e}")


def build_rollup_parallel(con, rollup_configs):
    """Build multiple rollups in parallel."""
    print(f"🟩 Building {len(rollup_configs)} rollups in parallel...")
    
    def build_single_rollup(config):
        table_name, sql = config
        try:
            start_time = time.time()
            con.execute(sql)
            duration = time.time() - start_time
            return table_name, duration, None
        except Exception as e:
            return table_name, 0, str(e)
    
    # Use ThreadPoolExecutor for I/O bound operations
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(build_single_rollup, config) for config in rollup_configs]
        
        results = []
        for future in as_completed(futures):
            table_name, duration, error = future.result()
            if error:
                print(f"   ❌ {table_name}: {error}")
            else:
                print(f"   ✓ {table_name}: {duration:.1f}s")
            results.append((table_name, duration, error))
    
    return results


def build_optimized_rollups(con):
    """Build all rollups with optimized configurations."""
    
    # Define rollup configurations
    rollup_configs = [
        # Day-level rollups
        (ROLLUP_BY_DAY, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_DAY} AS
            SELECT day, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY day, type
            ORDER BY day, type;
        """),
        
        (ROLLUP_BY_COUNTRY_DAY, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_COUNTRY_DAY} AS
            SELECT day, country, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY day, country, type
            ORDER BY day, country, type;
        """),
        
        (ROLLUP_BY_PUBLISHER_DAY, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_DAY} AS
            SELECT day, publisher_id, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY day, publisher_id, type
            ORDER BY day, publisher_id, type;
        """),
        
        (ROLLUP_BY_ADVERTISER_DAY, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER_DAY} AS
            SELECT day, advertiser_id, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY day, advertiser_id, type
            ORDER BY day, advertiser_id, type;
        """),
        
        (ROLLUP_BY_PUBLISHER_COUNTRY_DAY, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_COUNTRY_DAY} AS
            SELECT day, publisher_id, country, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY day, publisher_id, country, type
            ORDER BY day, publisher_id, country, type;
        """),
        
        # Time-level rollups
        (ROLLUP_BY_MINUTE, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_MINUTE} AS
            SELECT minute, day, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY minute, day, type
            ORDER BY minute, day, type;
        """),
        
        (ROLLUP_BY_HOUR, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_HOUR} AS
            SELECT hour, day, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY hour, day, type
            ORDER BY hour, day, type;
        """),
        
        (ROLLUP_BY_WEEK, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_WEEK} AS
            SELECT week, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY week, type
            ORDER BY week, type;
        """),
        
        # Dimension-only rollups
        (ROLLUP_BY_COUNTRY, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_COUNTRY} AS
            SELECT country, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY country, type
            ORDER BY country, type;
        """),
        
        (ROLLUP_BY_PUBLISHER, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER} AS
            SELECT publisher_id, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY publisher_id, type
            ORDER BY publisher_id, type;
        """),
        
        (ROLLUP_BY_ADVERTISER, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER} AS
            SELECT advertiser_id, type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY advertiser_id, type
            ORDER BY advertiser_id, type;
        """),
        
        # Type-only rollup (for queries that only filter by type)
        (ROLLUP_BY_TYPE_ONLY, f"""
            CREATE OR REPLACE TABLE {ROLLUP_BY_TYPE_ONLY} AS
            SELECT type,
                   COUNT(*) AS cnt,
                   SUM(bid_price) AS sum_bid,
                   SUM(total_price) AS sum_total
            FROM {PERSISTED_TABLE}
            GROUP BY type
            ORDER BY type;
        """),
    ]
    
    # Build rollups in parallel
    start_time = time.time()
    results = build_rollup_parallel(con, rollup_configs)
    total_time = time.time() - start_time
    
    print(f"🟩 All rollups completed in {total_time:.1f}s")
    
    # Create indexes
    create_indexes(con)
    
    return results


def optimize_database_settings(con):
    """Apply database optimizations."""
    print("🟩 Applying database optimizations...")
    
    optimizations = [
        "SET memory_limit='16GB';",
        "SET threads=8;",
        "SET preserve_insertion_order=false;",
        "SET enable_progress_bar=false;",
        "SET checkpoint_threshold='1GB';",
        "SET wal_autocheckpoint='1GB';",
    ]
    
    for opt in optimizations:
        try:
            con.execute(opt)
            print(f"   ✓ Applied: {opt}")
        except Exception as e:
            print(f"   ⚠️  Failed: {opt} - {e}")


def main():
    if not DB_PATH.exists():
        print(f"❌ Error: Database file not found at {DB_PATH}")
        print(f"   Please run step1_prepare_data.py first!")
        return

    con = duckdb.connect(DB_PATH)

    print("=" * 70)
    print("STEP 2: BUILD OPTIMIZED ROLLUPS")
    print("=" * 70)

    # Apply database optimizations
    optimize_database_settings(con)
    
    # Build optimized rollups
    total_start = time.time()
    results = build_optimized_rollups(con)
    total_time = time.time() - total_start

    con.close()

    print(f"\n✅ Step 2 complete! All optimized rollups created in {total_time:.1f}s")
    print(f"   Next: Run step3_run_queries.py")


if __name__ == "__main__":
    main()
