# Optimization Strategy (Without Z-Ordering)

## Why Z-Ordering Was Removed

**Decision:** Remove Z-ordering from the pipeline

**Reasoning:**
1. **O(n log n) complexity** - Sorting large datasets is expensive
2. **Rollups are primary optimization** - Most queries hit pre-aggregated tables
3. **Only helps edge cases** - Z-ordering only benefits queries that scan raw Parquet
4. **Not worth the cost** - The time spent sorting could be used for other optimizations

## Current Optimization Strategy

### 1. **Rollup Tables** (Primary Optimization) ⭐
**What:** Pre-aggregated tables for common query patterns
- `by_day`: Aggregated by day and type
- `by_country_day`: Aggregated by country, day, and type
- `by_publisher_day`: Aggregated by publisher, day, and type
- `by_minute`: Aggregated by minute for time-series queries

**Benefits:**
- Query speed: 10-100x faster than scanning raw data
- Query routing automatically selects the best rollup
- Handles 80-90% of typical queries

**Time cost:** ~60-120s to build all rollups

### 2. **Partitioned Parquet** (Storage Optimization)
**What:** Data split by type and day partitions

**Benefits:**
- Partition pruning: Skip irrelevant partitions during queries
- Parallel I/O: Read multiple partitions simultaneously
- Storage efficiency: Better compression per partition

**Optimizations applied:**
- `COMPRESSION 'zstd'`: Better compression than Snappy (30-50% smaller files)
- `ROW_GROUP_SIZE 200000`: Optimized for query performance
- `PARTITION_BY (type, day)`: Enables partition pruning

**Time cost:** ~30-90s to write (no sorting overhead!)

### 3. **Query Router** (Intelligent Source Selection)
**What:** Automatically picks the best data source

**How it works:**
1. Analyzes query patterns (GROUP BY, WHERE clauses)
2. Selects appropriate rollup table if possible
3. Falls back to Parquet only when necessary

**Benefits:**
- No manual query optimization needed
- Always uses the fastest available source
- Caching prevents redundant work

### 4. **LRU Cache with TTL** (Result Caching)
**What:** Caches query results with automatic eviction

**Benefits:**
- Repeated queries return instantly (0ms)
- Configurable cache size (default: 50MB)
- Time-to-live prevents stale results

**Trade-off:** Small memory overhead for big speed gains

### 5. **Database Optimizations** (DuckDB Settings)
```sql
SET preserve_insertion_order=false;  -- Faster inserts
SET threads=8;                        -- Parallel processing
SET memory_limit='14GB';              -- Use available RAM
```

## Performance Comparison

| Optimization | Query Speed | Write Speed | Storage Efficiency |
|-------------|-------------|-------------|-------------------|
| **No optimization** | Baseline | Baseline | Baseline |
| **Rollups only** | 10-100x faster | +60-120s | Minimal overhead |
| **Rollups + Parquet** | Same | +90-210s | 30-50% smaller |
| **+ Z-ordering** | Same | +180-300s | Same |

**Verdict:** Rollups + partitioned Parquet is optimal. Z-ordering adds cost without benefit since rollups handle most queries.

## Additional Optimizations You Could Consider

### 1. **Bloom Filters** (Future Enhancement)
Add bloom filters to Parquet files for faster predicate pushdown
- Best for: High-cardinality filters (publisher_id, advertiser_id)
- Cost: Minimal (small metadata addition)
- Benefit: 10-30% faster queries on Parquet

### 2. **Statistics Collection** (DuckDB Feature)
Collect column statistics for better query planning
```sql
ANALYZE events_persisted;
```
- Benefit: Better query plans from optimizer
- Cost: Few seconds to run

### 3. **Parallel Query Execution**
Already implemented! Uses ThreadPoolExecutor for concurrent queries
- Benefit: Multiple queries run simultaneously
- Trade-off: Higher memory usage

### 4. **Column Pruning** (Already Optimized)
Only reads columns needed by the query
- Parquet stores columns separately
- Unused columns aren't read from disk
- Automatic in DuckDB

### 5. **Predicate Pushdown** (Already Optimized)
Filters applied before data transfer
- WHERE clauses pushed to storage layer
- Only relevant rows loaded into memory
- Automatic in DuckDB

## Summary

**Current strategy (optimal):**
- ✅ Rollup tables for fast queries
- ✅ Partitioned Parquet for storage
- ✅ Query router for intelligent selection
- ✅ LRU cache for repeated queries
- ✅ ZSTD compression for smaller files
- ❌ Z-ordering (not worth the cost)

**Performance targets:**
- Prep time: 90-210s (under 7-minute limit)
- Query time: <1s for most queries (using rollups)
- Edge cases: 3-10s (scanning partitioned Parquet)
