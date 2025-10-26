# Optimal Strategy: 7-Minute Prep Time, 5-Minute Runtime

## Time Budget Analysis

**Prep Phase: 7 minutes (420 seconds)**
**Runtime Phase: 5 minutes (300 seconds)**

## Goal: Maximize Efficiency Within Constraints

## Time Budget Breakdown

### Prep Phase (7 minutes = 420 seconds)

```
1. Load CSV data:          10-15s
2. Create persisted table: 20-30s
3. Create rollups:         60-120s
4. Create Parquet:           60-180s  ← KEY VARIABLE!
─────────────────────────────────
Total:                       150-345s

Budget: 420s
Safety margin: 75-270s remaining ✅
```

### Runtime Phase (5 minutes = 300 seconds)

```
100 queries × average time:
- Rollup hits (80%): 0.1-1s each
- Edge cases (20%): 3-60s each

Worst case: 80 fast + 20 slow = 120-660s
Best case:  80 fast + 20 medium = 80-120s
```

## Optimization Strategy

### Phase 1: Smart Parquet Creation (Prep Phase)

**Option A: Basic Partitioning (Safest)**
```python
PARTITION_BY (type, day)
No Z-order
Snappy compression

Time: 60-90s
Benefit: Partition pruning
Safety: 330-360s remaining ✅
```

**Option B: Smart Z-Order (Balanced)**
```python
PARTITION_BY (type, day)
Z-order hot partition (impressions only)
Snappy compression

Time: 90-150s
Benefit: Edge cases 6-30x faster on impressions
Safety: 270-330s remaining ✅
```

**Option C: Full Z-Order (Maximum Performance)**
```python
PARTITION_BY (type, day)
Full Z-order (country, publisher_id, advertiser_id)
ZSTD compression level 3

Time: 180-240s
Benefit: All edge cases 6-30x faster
Safety: 180-240s remaining ⚠️ RISKY
```

### Phase 2: Rollup Optimization (Prep Phase)

**Create All 9 Rollups:** ✅ RECOMMENDED
```python
Time: 60-120s
Benefit: 80-90% queries hit rollups instantly
Worth it: YES ✅
```

**Create Additional Rollups:**
```python
# My optimized version adds:
- by_hour (for hour-level queries)
- by_week (for weekly analysis)
- by_type_only (for simple aggregations)

Time cost: +30-60s
Benefit: Better coverage for unknown queries
```

### Phase 3: Query Optimizations (Prep Phase)

**Add Strategic Indexes:**
```python
CREATE INDEX idx_by_day_day_type ON by_day(day, type);
CREATE INDEX idx_by_country_country_type ON by_country_day(country, day, type);
# ... etc

Time cost: +10-20s
Benefit: 2-5x faster rollup queries
Worth it: YES ✅
```

## My Recommended Strategy

### Prep Phase: Smart Optimization (380s target)

```python
1. Load & persist data:     30s
2. Create smart Parquet:    120s (Option B: smart Z-order)
3. Build all rollups:       120s (9 rollups + additional)
4. Add indexes:             20s
5. Buffer time:             130s remaining ✅
───────────────────────────────────────
Total: 380s (90% of 420s budget)
```

### Runtime Phase: Optimized Queries

```python
# Enhanced routing (already in my optimized files)
# Advanced caching (already in my optimized files)
# Parallel execution (optional, for bonus speed)

Expected: 80-150s for 100 queries ✅
```

## Implementation Plan

### High Priority Optimizations

**1. Smart Z-Order Parquet** ✅ HIGH IMPACT
```python
# Z-order only impression events (50% of data)
# Other events stream for speed
# Expected savings: 110-170s on edge cases
# Time cost: +60-90s in prep
# Net benefit: 20-110s saved!
```

**2. Strategic Indexes** ✅ HIGH IMPACT
```python
# Add indexes on all rollup tables
# Time cost: +10-20s
# Benefit: 2-5x faster rollup queries
# Net benefit: 40-200s saved on queries!
```

**3. Additional Rollups** ✅ MEDIUM IMPACT
```python
# by_hour, by_week, by_type_only
# Time cost: +30-60s
# Benefit: Better query coverage
# Reduces edge cases from 20% → 10-15%
# Net benefit: 60-300s saved!
```

**4. Enhanced Query Router** ✅ HIGH IMPACT
```python
# Better rollup selection logic
# Time cost: 0s (in prep, just code)
# Benefit: More queries hit rollups
# Net benefit: 30-150s saved!
```

**5. Advanced Caching** ✅ MEDIUM IMPACT
```python
# LRU cache with TTL
# Time cost: 0s (in prep)
# Benefit: Instant results for repeated queries
# Net benefit: 10-50s saved (if queries repeat)
```

### Low Priority Optimizations

**6. Parallel Rollup Creation** ⚠️ OPTIONAL
```python
# Create rollups in parallel
# Time cost: Same (parallel)
# Benefit: Better CPU utilization
# Risk: More memory usage
```

**7. Compression Optimization** ⚠️ MINOR BENEFIT
```python
# Use ZSTD instead of Snappy
# Time cost: +30-60s
# Benefit: Better compression (5x vs 3x)
# Risk: May push over 7-min limit
```

## Final Recommendation

### Optimal Strategy for 7-Min Prep, 5-Min Runtime

**Prep Phase (Target: 380s)**
```python
1. Basic data prep:          30s
2. Smart Z-order Parquet:    120s (impression events only)
3. All rollups (12 total):   120s (9 existing + 3 new)
4. Strategic indexes:         20s
5. Buffer time:              130s
─────────────────────────────────
Total: 420s (100% of budget, tight!)
```

**Runtime Phase (Target: 120s)**
```python
1. Rollup queries (85%):    85 × 0.5s = 42s
2. Edge cases (15%):        15 × 5s = 75s
3. Total queries:            117s ✅
─────────────────────────────────
Margin: 183s remaining ✅
```

## Expected Performance

### Without Optimizations
```
Prep: 150s (basic parquet + rollups)
Runtime: 200s (no optimizations)
Total: 350s
```

### With My Optimizations
```
Prep: 380s (smart Z-order + all optimizations)
Runtime: 120s (optimized queries)
Total: 500s
Improvement: 30% faster queries! ✅
```

## Risk Management

### Conservative Approach (Safer)
```python
Prep: 150-200s (basic optimizations only)
Runtime margin: 100-150s
Safety: High ✅
```

### Aggressive Approach (Maximum Performance)
```python
Prep: 380-420s (all optimizations)
Runtime margin: 20-40s
Safety: Tight ⚠️
```

### My Recommendation: **Balanced Approach** ✅
```python
Prep: 320-370s (most optimizations)
Runtime margin: 50-100s
Safety: Good ✅
Performance: Excellent ✅
```

