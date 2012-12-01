package com.dailycred.dynamonito.cache;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

import com.dailycred.dynamonito.core.CacheAdaptor;

/**
 * An in-memory cache using Guava Cache. Ideal for use during unit tests.
 * http://code.google.com/p/guava-libraries/wiki/CachesExplained
 * 
 */
public class InMemoryCacheAdaptor implements CacheAdaptor {

  private final Map<String, Cache<String, String>> cache;
  private boolean enabled;

  public InMemoryCacheAdaptor() {
    cache = Maps.newHashMap();
    enabled = true;
  }

  @Override
  public void put(String table, Object hashKey, Object rangeKey, String value, int ttl) {
    if (!enabled)
      return;
    final String key = key(hashKey, rangeKey);
    getTableCache(table, ttl).put(key, value);
  }

  @Override
  public String get(String table, Object hashKey, Object rangeKey) {
    if (!enabled)
      return null;
    Cache<String, String> tableCache = cache.get(table);
    if (tableCache == null)
      return null;
    final String key = key(hashKey, rangeKey);
    return tableCache.getIfPresent(key);
  }

  @Override
  public void remove(String table, Object hashKey, Object rangeKey) {
    if (!enabled)
      return;
    Cache<String, String> tableCache = cache.get(table);
    if (tableCache != null) {
      final String key = key(hashKey, rangeKey);
      tableCache.invalidate(key);
    }
  }

  @Override
  public void remove(String table) {
    Cache<String, String> tableCache = cache.get(table);
    if (tableCache != null) {
      tableCache.invalidateAll();
    }
  }

  @Override
  public void removeAll() {
    if (!enabled)
      return;
    cache.clear();
  }

  @Override
  public int count(String table) {
    if (!enabled)
      return -1;
    Cache<String, String> tableCache = cache.get(table);
    if (tableCache == null)
      return 0;
    else {
      long size = tableCache.size();
      if (size > Integer.MAX_VALUE)
        throw new RuntimeException("Count out of bounds");
      else
        return (int) size;
    }
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void setEnabled(boolean enableCache) {
    enabled = enableCache;
  }

  private Cache<String, String> getTableCache(String table, int ttl) {
    Cache<String, String> tableCache = cache.get(table);
    if (tableCache == null) {
      if (ttl == 0)
        tableCache = CacheBuilder.newBuilder().build();
      else
        tableCache = CacheBuilder.newBuilder().expireAfterWrite(ttl, TimeUnit.SECONDS).build();
      cache.put(table, tableCache);
    }
    return tableCache;
  }

  private String key(Object hashKey, Object rangeKey) {
    return rangeKey == null ? hashKey.toString() : hashKey.toString() + rangeKey.toString();
  }

}