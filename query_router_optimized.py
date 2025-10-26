#!/usr/bin/env python3
"""
Enhanced Query Router
---------------------
Optimized query routing with better rollup selection and caching.
"""

import json
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class QueryComplexity(Enum):
    SIMPLE = "simple"      # Can use rollups
    COMPLEX = "complex"    # Need raw data


@dataclass
class RollupConfig:
    name: str
    dimensions: Set[str]
    has_time_dimension: bool
    time_granularity: Optional[str] = None
    priority: int = 1  # Higher = better match


class OptimizedQueryRouter:
    """Enhanced query router with better rollup selection."""
    
    def __init__(self):
        self.rollup_configs = self._initialize_rollup_configs()
        self.cache = {}
        self.query_stats = {}
    
    def _initialize_rollup_configs(self) -> List[RollupConfig]:
        """Initialize rollup configurations with priorities."""
        return [
            # High-priority rollups (exact matches)
            RollupConfig("by_day", {"day", "type"}, True, "day", 10),
            RollupConfig("by_country_day", {"day", "country", "type"}, True, "day", 10),
            RollupConfig("by_publisher_day", {"day", "publisher_id", "type"}, True, "day", 10),
            RollupConfig("by_advertiser_day", {"day", "advertiser_id", "type"}, True, "day", 10),
            RollupConfig("by_publisher_country_day", {"day", "publisher_id", "country", "type"}, True, "day", 10),
            
            # Time-granular rollups
            RollupConfig("by_minute", {"minute", "day", "type"}, True, "minute", 9),
            RollupConfig("by_hour", {"hour", "day", "type"}, True, "hour", 9),
            RollupConfig("by_week", {"week", "type"}, True, "week", 9),
            
            # Dimension-only rollups
            RollupConfig("by_country", {"country", "type"}, False, None, 8),
            RollupConfig("by_publisher", {"publisher_id", "type"}, False, None, 8),
            RollupConfig("by_advertiser", {"advertiser_id", "type"}, False, None, 8),
            RollupConfig("by_type_only", {"type"}, False, None, 7),
        ]
    
    def analyze_query(self, query: Dict) -> Tuple[QueryComplexity, Set[str], Set[str]]:
        """Analyze query complexity and extract dimensions."""
        select_items = query.get("select", [])
        group_by = set(query.get("group_by", []))
        where_conditions = query.get("where", [])
        
        # Extract columns from WHERE clause
        where_cols = {cond["col"] for cond in where_conditions}
        
        # Check if query can use rollups
        complexity = self._determine_complexity(select_items, group_by, where_cols)
        
        return complexity, group_by, where_cols
    
    def _determine_complexity(self, select_items: List, group_by: Set[str], where_cols: Set[str]) -> QueryComplexity:
        """Determine if query can use rollups."""
        # Check for supported aggregate functions
        allowed_aggs = {"COUNT", "SUM", "AVG"}
        for item in select_items:
            if isinstance(item, dict):
                func, _ = next(iter(item.items()))
                if func.upper() not in allowed_aggs:
                    return QueryComplexity.COMPLEX
        
        # Check for unsupported columns in GROUP BY
        unsupported_cols = {"ts", "auction_id", "user_id"}
        if group_by & unsupported_cols:
            return QueryComplexity.COMPLEX
        
        return QueryComplexity.SIMPLE
    
    def find_best_rollup(self, group_by: Set[str], where_cols: Set[str]) -> Optional[str]:
        """Find the best rollup for the query."""
        best_match = None
        best_score = 0
        
        for config in self.rollup_configs:
            score = self._calculate_match_score(config, group_by, where_cols)
            if score > best_score:
                best_score = score
                best_match = config.name
        
        return best_match if best_score > 0 else None
    
    def _calculate_match_score(self, config: RollupConfig, group_by: Set[str], where_cols: Set[str]) -> int:
        """Calculate how well a rollup matches the query."""
        score = 0
        
        # Check if all GROUP BY columns are in rollup
        if group_by.issubset(config.dimensions):
            score += config.priority * 10
        
        # Check if WHERE columns are supported by rollup
        supported_where_cols = where_cols & config.dimensions
        if supported_where_cols:
            score += len(supported_where_cols) * 2
        
        # Penalty for unsupported WHERE columns
        unsupported_where_cols = where_cols - config.dimensions
        if unsupported_where_cols:
            score -= len(unsupported_where_cols) * 5
        
        # Bonus for exact dimension match
        if group_by == config.dimensions:
            score += 20
        
        return score
    
    def route_query(self, query: Dict) -> str:
        """Route query to optimal data source."""
        # Check cache first
        cache_key = self._normalize_query_for_cache(query)
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Analyze query
        complexity, group_by, where_cols = self.analyze_query(query)
        
        if complexity == QueryComplexity.COMPLEX:
            result = "events_parquet"
        else:
            # Try to find best rollup
            best_rollup = self.find_best_rollup(group_by, where_cols)
            result = best_rollup or "events_parquet"
        
        # Cache result
        self.cache[cache_key] = result
        
        # Update stats
        self._update_stats(result, complexity)
        
        return result
    
    def _normalize_query_for_cache(self, query: Dict) -> str:
        """Normalize query for caching."""
        def normalize(obj):
            if isinstance(obj, dict):
                return {k: normalize(obj[k]) for k in sorted(obj)}
            if isinstance(obj, list):
                return [normalize(v) for v in obj]
            return obj
        return json.dumps(normalize(query), separators=(",", ":"))
    
    def _update_stats(self, source: str, complexity: QueryComplexity):
        """Update query statistics."""
        if source not in self.query_stats:
            self.query_stats[source] = {"count": 0, "complexity": complexity}
        self.query_stats[source]["count"] += 1
    
    def get_stats(self) -> Dict:
        """Get routing statistics."""
        return {
            "cache_hits": len(self.cache),
            "query_stats": self.query_stats,
            "rollup_configs": len(self.rollup_configs)
        }
    
    def clear_cache(self):
        """Clear query cache."""
        self.cache.clear()


# Global router instance
router = OptimizedQueryRouter()


def pick_optimal_source(query: Dict) -> str:
    """Enhanced query routing function."""
    return router.route_query(query)


def get_routing_stats() -> Dict:
    """Get routing statistics."""
    return router.get_stats()


def clear_routing_cache():
    """Clear routing cache."""
    router.clear_cache()
