package com.dailycred.dynamonito.cache;

import net.spy.memcached.MemcachedClientIF;

import com.dailycred.dynamonito.core.CacheAdaptor;

public class ElastiCacheCCAdaptor implements CacheAdaptor {

	protected final MemcachedClientIF cache;
	protected boolean enabled;

	public ElastiCacheCCAdaptor(MemcachedClientIF cache) {
		this.cache = cache;
		this.enabled = true;
	}

	@Override
	public void put(String table, Object hashKey, Object rangeKey,
			String value, int ttl) {
		String key = keyFor(table, hashKey, rangeKey);
		cache.set(key, ttl, value);
	}

	@Override
	public String get(String table, Object hashKey, Object rangeKey) {
		String key = keyFor(table, hashKey, rangeKey);
		return cache.get(key).toString();
	}

	@Override
	public void remove(String table, Object hashKey, Object rangeKey) {
		String key = keyFor(table, hashKey, rangeKey);
		cache.delete(key);
	}

	@Override
	public void remove(String table) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeAll() {
		cache.flush();
	}

	@Override
	public int count(String table) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEnabled() {
		return enabled;
	}

	@Override
	public void setEnabled(boolean enableCache) {
		enabled = enableCache;
	}

	protected String keyFor(String table, Object hashKey, Object rangeKey) {
		return String.format("%s.%s-%s", table, hashKey.toString(), rangeKey.toString());
	}

}
