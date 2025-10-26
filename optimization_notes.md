# Query Engine Optimization Notes

## Z-Ordering Analysis

### ❌ DON'T Add Z-Ordering to Parquet

**Why not:**
1. **Partition pruning** already handles your query patterns efficiently
2. **Rollups** handle complex multi-dimensional queries better
3. **Write time** would increase 10-15x (43s → 5-10min)
4. **No significant benefit** for your query patterns

### ✅ Current Approach is Optimal

**Parquet Strategy:**
- `PARTITION_BY (type, day)` - Perfect for your queries
- No ORDER BY - Fastest write performance
- Partition pruning - Handles filters efficiently

**Rollup Strategy:**
- Pre-aggregated - Instant results for common patterns
- Multiple rollups - Covers different dimension combinations
- Indexed - Faster lookups (if using optimized version)

### Best Optimizations

1. **Strategic Indexing** - Add indexes to rollup tables
2. **Enhanced Routing** - Better rollup selection
3. **Advanced Caching** - LRU cache with TTL
4. **Parallel Processing** - Faster rollup creation

**Don't waste time on Z-ordering Parquet!**

