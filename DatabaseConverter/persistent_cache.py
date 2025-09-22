import json
import gzip
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import hashlib

class PersistentCache:
    """
    Persistent file-based cache with compression, size limits, and expiration.
    Designed to handle large datasets efficiently while surviving page refreshes.
    """
    
    def __init__(self, cache_dir: str = "cache", max_size_mb: int = 500, default_expiry_hours: int = 24):
        self.cache_dir = cache_dir
        self.max_size_mb = max_size_mb
        self.default_expiry_hours = default_expiry_hours
        
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
        
        # Cache metadata file
        self.metadata_file = os.path.join(cache_dir, "cache_metadata.json")
        self.metadata = self._load_metadata()
    
    def _load_metadata(self) -> Dict[str, Any]:
        """Load cache metadata or create new if doesn't exist"""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except:
                return {"entries": {}, "total_size_mb": 0}
        return {"entries": {}, "total_size_mb": 0}
    
    def _save_metadata(self):
        """Save cache metadata to disk"""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
    
    def _generate_cache_key(self, search_criteria: str) -> str:
        """Generate a unique cache key from search criteria"""
        return hashlib.md5(search_criteria.encode()).hexdigest()
    
    def _get_cache_file_path(self, cache_key: str) -> str:
        """Get the file path for a cache key"""
        return os.path.join(self.cache_dir, f"{cache_key}.gz")
    
    def _is_expired(self, cache_key: str) -> bool:
        """Check if a cache entry is expired"""
        if cache_key not in self.metadata["entries"]:
            return True
        
        entry = self.metadata["entries"][cache_key]
        created_time = datetime.fromisoformat(entry["created"])
        expiry_time = created_time + timedelta(hours=entry.get("expiry_hours", self.default_expiry_hours))
        
        return datetime.now() > expiry_time
    
    def _get_file_size_mb(self, file_path: str) -> float:
        """Get file size in MB"""
        if os.path.exists(file_path):
            return os.path.getsize(file_path) / (1024 * 1024)
        return 0
    
    def _cleanup_expired_entries(self):
        """Remove expired cache entries"""
        expired_keys = []
        for cache_key in list(self.metadata["entries"].keys()):
            if self._is_expired(cache_key):
                expired_keys.append(cache_key)
        
        for cache_key in expired_keys:
            self._remove_cache_entry(cache_key)
    
    def _cleanup_by_size(self):
        """Remove oldest entries if cache exceeds size limit"""
        while self.metadata["total_size_mb"] > self.max_size_mb:
            # Find oldest entry
            oldest_key = None
            oldest_time = None
            
            for cache_key, entry in self.metadata["entries"].items():
                created_time = datetime.fromisoformat(entry["created"])
                if oldest_time is None or created_time < oldest_time:
                    oldest_time = created_time
                    oldest_key = cache_key
            
            if oldest_key:
                self._remove_cache_entry(oldest_key)
            else:
                break
    
    def _remove_cache_entry(self, cache_key: str):
        """Remove a single cache entry"""
        if cache_key in self.metadata["entries"]:
            # Remove file
            file_path = self._get_cache_file_path(cache_key)
            if os.path.exists(file_path):
                file_size = self._get_file_size_mb(file_path)
                os.remove(file_path)
                self.metadata["total_size_mb"] -= file_size
            
            # Remove from metadata
            del self.metadata["entries"][cache_key]
            self._save_metadata()
    
    def get(self, search_criteria: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve cached data for given search criteria.
        Returns None if not found or expired.
        """
        cache_key = self._generate_cache_key(search_criteria)
        
        # Check if exists and not expired
        if cache_key not in self.metadata["entries"] or self._is_expired(cache_key):
            return None
        
        # Load compressed data
        file_path = self._get_cache_file_path(cache_key)
        if not os.path.exists(file_path):
            # File missing, remove from metadata
            if cache_key in self.metadata["entries"]:
                del self.metadata["entries"][cache_key]
                self._save_metadata()
            return None
        
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
            
            # Update access time
            self.metadata["entries"][cache_key]["last_accessed"] = datetime.now().isoformat()
            self._save_metadata()
            
            return data
        except Exception as e:
            # Corrupted file, remove it
            self._remove_cache_entry(cache_key)
            return None
    
    def set(self, search_criteria: str, data: List[Dict[str, Any]], expiry_hours: Optional[int] = None):
        """
        Store data in cache with compression.
        """
        cache_key = self._generate_cache_key(search_criteria)
        file_path = self._get_cache_file_path(cache_key)
        
        # Clean up expired entries first
        self._cleanup_expired_entries()
        
        # Remove existing entry if present
        if cache_key in self.metadata["entries"]:
            self._remove_cache_entry(cache_key)
        
        # Save compressed data
        try:
            with gzip.open(file_path, 'wt', encoding='utf-8') as f:
                json.dump(data, f, separators=(',', ':'))  # Compact JSON
            
            # Update metadata
            file_size = self._get_file_size_mb(file_path)
            self.metadata["entries"][cache_key] = {
                "created": datetime.now().isoformat(),
                "last_accessed": datetime.now().isoformat(),
                "size_mb": file_size,
                "expiry_hours": expiry_hours or self.default_expiry_hours,
                "search_criteria": search_criteria
            }
            self.metadata["total_size_mb"] += file_size
            
            # Clean up by size if needed
            self._cleanup_by_size()
            
            self._save_metadata()
            
        except Exception as e:
            # Clean up partial file
            if os.path.exists(file_path):
                os.remove(file_path)
            raise e
    
    def has(self, search_criteria: str) -> bool:
        """Check if data exists in cache and is not expired"""
        cache_key = self._generate_cache_key(search_criteria)
        return cache_key in self.metadata["entries"] and not self._is_expired(cache_key)
    
    def clear(self):
        """Clear all cache data"""
        for cache_key in list(self.metadata["entries"].keys()):
            self._remove_cache_entry(cache_key)
        self.metadata = {"entries": {}, "total_size_mb": 0}
        self._save_metadata()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        # Clean up expired entries for accurate stats
        self._cleanup_expired_entries()
        
        total_entries = len(self.metadata["entries"])
        total_size_mb = self.metadata["total_size_mb"]
        
        # Calculate age distribution
        now = datetime.now()
        recent_count = 0  # < 1 hour
        fresh_count = 0   # < 6 hours
        old_count = 0     # >= 6 hours
        
        for entry in self.metadata["entries"].values():
            created = datetime.fromisoformat(entry["created"])
            age_hours = (now - created).total_seconds() / 3600
            
            if age_hours < 1:
                recent_count += 1
            elif age_hours < 6:
                fresh_count += 1
            else:
                old_count += 1
        
        return {
            "total_entries": total_entries,
            "total_size_mb": round(total_size_mb, 2),
            "max_size_mb": self.max_size_mb,
            "usage_percent": round((total_size_mb / self.max_size_mb) * 100, 1),
            "age_distribution": {
                "recent_1h": recent_count,
                "fresh_6h": fresh_count,
                "older": old_count
            }
        }