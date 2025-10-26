#!/usr/bin/env python3
"""
Step 3: Run Queries (Optimized)
-------------------------------
Enhanced query execution with optimized routing, caching, and performance monitoring.

Key optimizations:
- Advanced query routing
- LRU cache with TTL
- Performance monitoring
- Parallel query execution
- Smart rollup selection
"""

import duckdb
import time
from pathlib import Path
import argparse
import json
import copy
import csv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple

from assembler import assemble_sql
from inputs import queries
from query_router_optimized import pick_optimal_source, get_routing_stats, clear_routing_cache
from advanced_cache import get_cached_result, cache_result, get_cache_stats, clear_cache

# Configuration
DB_PATH = Path("tmp/baseline.duckdb")

# Rollup table names
ROLLUP_TABLES = {
    "by_day", "by_country_day", "by_publisher_day", "by_advertiser_day",
    "by_publisher_country_day", "by_minute", "by_hour", "by_week",
    "by_country", "by_publisher", "by_advertiser", "by_type_only"
}


def optimize_database_connection(con):
    """Apply database optimizations for query execution."""
    optimizations = [
        "SET memory_limit='16GB';",
        "SET threads=8;",
        "SET preserve_insertion_order=false;",
        "SET enable_progress_bar=false;",
        "SET checkpoint_threshold='1GB';",
        "SET wal_autocheckpoint='1GB';",
        "SET enable_optimizer=true;",
        "SET enable_verification=false;",  # Skip verification for speed
    ]
    
    for opt in optimizations:
        try:
            con.execute(opt)
        except Exception as e:
            print(f"   ⚠️  Optimization failed: {opt} - {e}")


def _where_clause(where):
    """Generate WHERE clause from conditions."""
    if not where:
        return ""
    parts = []
    for cond in where:
        col, op, val = cond["col"], cond["op"], cond["val"]
        if op == "eq":
            parts.append(f"{col} = '{val}'")
        elif op == "neq":
            parts.append(f"{col} != '{val}'")
        elif op in ("lt", "lte", "gt", "gte"):
            sym = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[op]
            parts.append(f"{col} {sym} {val}")
        elif op == "between":
            low, high = val
            parts.append(f"{col} BETWEEN '{low}' AND '{high}'")
        elif op == "in":
            vals = ", ".join(f"'{v}'" for v in val)
            parts.append(f"{col} IN ({vals})")
    return "WHERE " + " AND ".join(parts)


def _assemble_rollup_sql(q: dict, rollup_table: str) -> str:
    """Generate SQL for rollup table queries."""
    select_parts = []
    for item in q.get("select", []):
        if isinstance(item, str):
            select_parts.append(item)
        elif isinstance(item, dict):
            func, col = next(iter(item.items()))
            F = func.upper()
            if F == "COUNT":
                expr = "SUM(cnt)"
                alias = "COUNT(*)"
            elif F == "SUM":
                if col == "bid_price":
                    expr = "SUM(sum_bid)"
                elif col == "total_price":
                    expr = "SUM(sum_total)"
                else:
                    expr = f"SUM({col})"
                alias = f"SUM({col})"
            elif F == "AVG":
                if col == "bid_price":
                    expr = "SUM(sum_bid) * 1.0 / NULLIF(SUM(cnt), 0)"
                elif col == "total_price":
                    expr = "SUM(sum_total) * 1.0 / NULLIF(SUM(cnt), 0)"
                else:
                    expr = f"AVG({col})"
                alias = f"AVG({col})"
            else:
                expr = f"{F}({col})"
                alias = f"{F}({col})"
            select_parts.append(f"{expr} AS \"{alias}\"")

    where_sql = _where_clause(q.get("where"))
    group_by_cols = q.get("group_by") or []
    group_by_sql = ("GROUP BY " + ", ".join(group_by_cols)) if group_by_cols else ""
    order_by_items = q.get("order_by") or []
    order_by_sql = ""
    if order_by_items:
        parts = []
        for o in order_by_items:
            col = o['col']
            if '(' in col:
                parts.append(f'"{col}" {o.get("dir", "asc").upper()}')
            else:
                parts.append(f'{col} {o.get("dir", "asc").upper()}')
        order_by_sql = "ORDER BY " + ", ".join(parts)

    return (
        f"SELECT {', '.join(select_parts)} FROM {rollup_table} "
        f"{where_sql} {group_by_sql} {order_by_sql}"
    ).strip()


def execute_query_optimized(con, query: dict, query_num: int) -> Tuple[float, List, List, str]:
    """Execute single query with optimizations."""
    # Check cache first
    cached_result = get_cached_result(query)
    if cached_result:
        cols, rows = cached_result
        return 0.0, cols, rows, "CACHED"
    
    # Route to optimal source
    source = pick_optimal_source(query)
    
    # Generate SQL
    if source in ROLLUP_TABLES:
        sql = _assemble_rollup_sql(query, source)
        source_type = f"ROLLUP({source})"
    else:
        sql = assemble_sql(query)
        source_type = f"TABLE({source})"
    
    # Execute query
    start_time = time.time()
    try:
        res = con.execute(sql)
        cols = [d[0].lower().replace('count(*)', 'count_star()') for d in res.description]
        rows = res.fetchall()
        execution_time = time.time() - start_time
        
        # Cache result
        cache_result(query, (cols, rows))
        
        return execution_time, cols, rows, source_type
        
    except Exception as e:
        print(f"   ❌ Query {query_num} failed: {e}")
        return 0.0, [], [], f"ERROR({str(e)[:50]})"


def run_queries_parallel(con, queries_list: List[dict], out_dir: Path, truth_dir: Path = None) -> List[dict]:
    """Execute queries in parallel for better performance."""
    results = []
    
    def execute_single_query(query_data):
        query_num, query = query_data
        return query_num, execute_query_optimized(con, query, query_num)
    
    # Execute queries in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        query_data = [(i, q) for i, q in enumerate(queries_list, 1)]
        futures = [executor.submit(execute_single_query, data) for data in query_data]
        
        for future in as_completed(futures):
            query_num, (execution_time, cols, rows, source_type) = future.result()
            
            print(f"\n🟦 Query {query_num} ({source_type}):")
            print(f"   ✅ Rows: {len(rows):,} | Time: {execution_time:.3f}s")
            
            # Write results to CSV
            csv_path = out_dir / f"q{query_num}.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            print(f"   💾 Saved to {csv_path}")
            
            results.append({
                "query": query_num,
                "rows": len(rows),
                "time": execution_time,
                "source": source_type,
                "cached": source_type == "CACHED"
            })
    
    return results


def run_queries_sequential(con, queries_list: List[dict], out_dir: Path, truth_dir: Path = None) -> List[dict]:
    """Execute queries sequentially with optimizations."""
    results = []
    
    for i, q in enumerate(queries_list, 1):
        execution_time, cols, rows, source_type = execute_query_optimized(con, q, i)
        
        print(f"\n🟦 Query {i} ({source_type}):")
        print(f"   ✅ Rows: {len(rows):,} | Time: {execution_time:.3f}s")
        
        # Write results to CSV
        csv_path = out_dir / f"q{i}.csv"
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)
        print(f"   💾 Saved to {csv_path}")
        
        results.append({
            "query": i,
            "rows": len(rows),
            "time": execution_time,
            "source": source_type,
            "cached": source_type == "CACHED"
        })
    
    return results


def print_performance_summary(results: List[dict]):
    """Print detailed performance summary."""
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)
    
    total_time = sum(r['time'] for r in results)
    cached_queries = sum(1 for r in results if r['cached'])
    
    for r in results:
        cache_indicator = " 🚀" if r['cached'] else ""
        print(f"Q{r['query']}: {r['time']:.3f}s | {r['rows']:,} rows | {r['source']}{cache_indicator}")
    
    print("-" * 80)
    print(f"Total time: {total_time:.3f}s")
    print(f"Cached queries: {cached_queries}/{len(results)}")
    print(f"Average time per query: {total_time/len(results):.3f}s")
    
    # Print cache statistics
    cache_stats = get_cache_stats()
    print(f"Cache hit rate: {cache_stats['hit_rate']:.1%}")
    print(f"Cache size: {cache_stats['current_size_mb']:.1f}MB")


def main():
    parser = argparse.ArgumentParser(
        description="Step 3: Run queries with advanced optimizations"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory to save query results (CSV files)"
    )
    parser.add_argument(
        "--truth-dir",
        type=Path,
        help="Directory with ground truth CSV files for comparison (optional)"
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Execute queries in parallel (experimental)"
    )

    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"❌ Error: Database file not found at {DB_PATH}")
        print(f"   Please run step1_prepare_data.py and step2_build_rollups.py first!")
        return

    # Ensure output directory exists
    args.out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)
    
    # Apply database optimizations
    optimize_database_connection(con)

    print("=" * 80)
    print("STEP 3: RUN OPTIMIZED QUERIES")
    print("=" * 80)
    print(f"📁 Output directory: {args.out_dir}")
    if args.truth_dir:
        print(f"🔍 Truth directory: {args.truth_dir}")
    print(f"🚀 Parallel execution: {'Yes' if args.parallel else 'No'}")

    # Clear caches for fresh start
    clear_cache()
    clear_routing_cache()

    # Execute queries
    start_time = time.time()
    if args.parallel:
        results = run_queries_parallel(con, queries, args.out_dir, args.truth_dir)
    else:
        results = run_queries_sequential(con, queries, args.out_dir, args.truth_dir)
    total_time = time.time() - start_time

    con.close()

    # Print performance summary
    print_performance_summary(results)
    
    # Print routing statistics
    routing_stats = get_routing_stats()
    print(f"\nRouting stats: {routing_stats['cache_hits']} cached routes")
    
    print("=" * 80)
    print(f"\n✅ All queries complete! Results saved to {args.out_dir}")
    print(f"Total execution time: {total_time:.3f}s")


if __name__ == "__main__":
    main()
