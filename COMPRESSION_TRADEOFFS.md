# Parquet Compression and Bloom Filter Tradeoffs

## The Issue: ZSTD vs SNAPPY

When implementing bloom filters, we tested two compression strategies:

### ZSTD (Originally Implemented)
- ✅ **Better compression** (~30-40% smaller files)
- ❌ **Slower reads** (2-3x slower decompression)
- ✅ **Good for:** Storage-constrained systems, cold storage
- ❌ **Bad for:** Query performance (your use case)

### SNAPPY (Current Implementation)
- ✅ **Faster reads** (2-3x faster decompression)
- ❌ **Larger files** (~30-40% bigger than ZSTD)
- ✅ **Good for:** Query performance, hot data
- ✅ **Still better than uncompressed** (~50% smaller than raw)

## Performance Results

### Test 1: ZSTD Compression (Slower)
```
Q1: 3.116s | Q2: 0.708s | Q3: 0.003s | Q4: 1.010s | Q5: 3.062s
Total: 7.899s
```

### Test 2: Snappy Compression (Expected)
```
Q1: 1.085s | Q2: 0.568s | Q3: 0.077s | Q4: 0.657s | Q5: 0.560s
Total: 2.947s (2.7x faster than ZSTD!)
```

## Why Bloom Filters Work Best with SNAPPY

1. **Bloom filters are metadata** - they don't compress data
2. **Your queries scan data** - decompression speed matters more than file size
3. **Partitioning already reduces I/O** - compression savings are less important
4. **High-cardinality columns get bloom filters automatically** - no configuration needed

## Configuration Rationale

### Row Group Size: 122,880 (DuckDB Default)
- This is DuckDB's default and is already optimized
- Bloom filters work well at this granularity
- Provides good balance between:
  - File metadata size
  - Parallel processing
  - Bloom filter effectiveness

### Why NOT Smaller Row Groups?
- More bloom filters = more metadata = slower reads
- Diminishing returns for your query patterns

### Why NOT Larger Row Groups?
- Less parallelism
- Bloom filters less effective (more false positives)

## How Bloom Filters are Applied (Automatic)

DuckDB 1.2.0+ automatically applies bloom filters based on:

1. **Dictionary Encoding Threshold**
   - Default: 10% of row group size (~12,288 distinct values)
   - If column has fewer distinct values → dictionary encoded → bloom filter added

2. **Column Types**
   - String columns: Usually get bloom filters
   - Integer IDs: Usually get bloom filters
   - Low cardinality (like `type`): Uses dictionary encoding only (no bloom filter needed)

3. **Your Columns**
   - `country`: ~200 values → ✅ Gets bloom filter
   - `publisher_id`: ~1000s values → ✅ Gets bloom filter
   - `advertiser_id`: ~1000s values → ✅ Gets bloom filter
   - `auction_id`: High cardinality → ✅ Gets bloom filter
   - `type`: 3-4 values → ❌ Too low cardinality (dictionary only)

## Recommendations by Use Case

### For Your Workload (Frequent Queries)
```python
COMPRESSION 'SNAPPY',      # Fast reads
ROW_GROUP_SIZE 122880      # Default, balanced
# Bloom filters automatic
```
**Result:** Fast queries, slightly larger files

### For Storage-Optimized (Cold Data)
```python
COMPRESSION 'ZSTD',        # Better compression
ROW_GROUP_SIZE 122880      # Default
# Bloom filters automatic
```
**Result:** Smaller files, slower queries

### For No Compression (Maximum Speed)
```python
COMPRESSION 'UNCOMPRESSED', # Fastest reads
ROW_GROUP_SIZE 122880       # Default
# Bloom filters automatic
```
**Result:** Fastest queries, largest files

## Switching Compression Types

To change compression, just regenerate Parquet files:

```bash
# Edit convert_csv_to_parquet.py, change COMPRESSION to:
# 'SNAPPY'   - Fast reads (recommended)
# 'ZSTD'     - Better compression
# 'GZIP'     - Middle ground
# 'UNCOMPRESSED' - Fastest reads, largest files

# Then regenerate:
rm -rf ./output/events
python convert_csv_to_parquet.py \
  --data-dir /Users/matthewhu/Downloads/data \
  --out-dir ./output
```

## Key Takeaways

1. ✅ **SNAPPY is optimal for your workload** (frequent queries)
2. ✅ **Bloom filters are automatic** - only applied to high-cardinality columns
3. ✅ **No configuration overhead** - DuckDB decides intelligently
4. ✅ **Partitioning + SNAPPY + Bloom = Best performance for your queries**
5. ❌ **Don't use ZSTD** unless storage is more important than query speed

## File Size Comparison (Estimated)

| Compression | File Size | Read Speed | Write Speed |
|------------|-----------|------------|-------------|
| UNCOMPRESSED | 100 GB | 1.0x (fastest) | 1.0x (fastest) |
| SNAPPY | 50 GB | 0.9x | 0.95x |
| GZIP | 35 GB | 0.5x | 0.6x |
| ZSTD | 30 GB | 0.3x (slowest) | 0.4x |

**Your choice: SNAPPY** - Best balance for query performance! 🎯

