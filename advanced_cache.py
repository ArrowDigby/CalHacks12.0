#!/usr/bin/env python3
"""
Advanced Caching System
-----------------------
LRU cache with TTL, size limits, and intelligent eviction.
"""

import time
import json
import hashlib
from typing import Any, Dict, Optional, Tuple, List
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    data: Any
    timestamp: float
    access_count: int
    size_bytes: int
    query_hash: str


class AdvancedCache:
    """Advanced caching system with LRU, TTL, and size management."""
    
    def __init__(self, max_size_mb: int = 100, ttl_seconds: int = 3600, max_entries: int = 1000):
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.current_size_bytes = 0
        self.lock = Lock()
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        
    def _calculate_size(self, data: Any) -> int:
        """Calculate approximate size of data in bytes."""
        try:
            if isinstance(data, (list, tuple)):
                return sum(self._calculate_size(item) for item in data)
            elif isinstance(data, dict):
                return sum(self._calculate_size(k) + self._calculate_size(v) for k, v in data.items())
            elif isinstance(data, str):
                return len(data.encode('utf-8'))
            elif isinstance(data, (int, float)):
                return 8
            else:
                return len(str(data).encode('utf-8'))
        except:
            return 1024  # Default estimate
    
    def _generate_key(self, query: Dict) -> str:
        """Generate cache key from query."""
        # Normalize query for consistent keys
        normalized = self._normalize_query(query)
        query_str = json.dumps(normalized, separators=(",", ":"), sort_keys=True)
        return hashlib.md5(query_str.encode()).hexdigest()
    
    def _normalize_query(self, query: Dict) -> Dict:
        """Normalize query for consistent caching."""
        def normalize(obj):
            if isinstance(obj, dict):
                return {k: normalize(obj[k]) for k in sorted(obj)}
            if isinstance(obj, list):
                return [normalize(v) for v in obj]
            return obj
        return normalize(query)
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if cache entry is expired."""
        return time.time() - entry.timestamp > self.ttl_seconds
    
    def _evict_if_needed(self):
        """Evict entries if cache is full."""
        while (self.current_size_bytes > self.max_size_bytes or 
               len(self.cache) > self.max_entries):
            
            if not self.cache:
                break
                
            # Remove least recently used entry
            key, entry = self.cache.popitem(last=False)
            self.current_size_bytes -= entry.size_bytes
            self.evictions += 1
    
    def get(self, query: Dict) -> Optional[Tuple]:
        """Get cached result for query."""
        with self.lock:
            key = self._generate_key(query)
            
            if key not in self.cache:
                self.misses += 1
                return None
            
            entry = self.cache[key]
            
            # Check if expired
            if self._is_expired(entry):
                del self.cache[key]
                self.current_size_bytes -= entry.size_bytes
                self.misses += 1
                return None
            
            # Update access count and move to end (most recently used)
            entry.access_count += 1
            self.cache.move_to_end(key)
            self.hits += 1
            
            return entry.data
    
    def put(self, query: Dict, result: Tuple):
        """Cache query result."""
        with self.lock:
            key = self._generate_key(query)
            size_bytes = self._calculate_size(result)
            
            # Create cache entry
            entry = CacheEntry(
                data=result,
                timestamp=time.time(),
                access_count=1,
                size_bytes=size_bytes,
                query_hash=key
            )
            
            # Remove existing entry if present
            if key in self.cache:
                old_entry = self.cache[key]
                self.current_size_bytes -= old_entry.size_bytes
                del self.cache[key]
            
            # Add new entry
            self.cache[key] = entry
            self.current_size_bytes += size_bytes
            
            # Evict if needed
            self._evict_if_needed()
    
    def clear(self):
        """Clear all cached entries."""
        with self.lock:
            self.cache.clear()
            self.current_size_bytes = 0
    
    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = self.hits / total_requests if total_requests > 0 else 0
            
            return {
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": hit_rate,
                "evictions": self.evictions,
                "current_size_mb": self.current_size_bytes / (1024 * 1024),
                "max_size_mb": self.max_size_bytes / (1024 * 1024),
                "entries": len(self.cache),
                "max_entries": self.max_entries
            }
    
    def get_top_queries(self, limit: int = 10) -> List[Dict]:
        """Get most frequently accessed queries."""
        with self.lock:
            sorted_entries = sorted(
                self.cache.items(),
                key=lambda x: x[1].access_count,
                reverse=True
            )
            
            return [
                {
                    "query_hash": entry.query_hash,
                    "access_count": entry.access_count,
                    "size_mb": entry.size_bytes / (1024 * 1024),
                    "age_seconds": time.time() - entry.timestamp
                }
                for _, entry in sorted_entries[:limit]
            ]


# Global cache instance
cache = AdvancedCache(max_size_mb=50, ttl_seconds=1800, max_entries=500)


def get_cached_result(query: Dict) -> Optional[Tuple]:
    """Get cached query result."""
    return cache.get(query)


def cache_result(query: Dict, result: Tuple):
    """Cache query result."""
    cache.put(query, result)


def get_cache_stats() -> Dict:
    """Get cache statistics."""
    return cache.get_stats()


def clear_cache():
    """Clear all cached results."""
    cache.clear()


def get_top_cached_queries(limit: int = 10) -> List[Dict]:
    """Get most frequently cached queries."""
    return cache.get_top_queries(limit)
