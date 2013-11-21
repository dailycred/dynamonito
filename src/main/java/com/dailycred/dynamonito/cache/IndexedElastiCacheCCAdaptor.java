package com.dailycred.dynamonito.cache;

import java.util.List;

import com.dailycred.dynamonito.cache.elastic.TableKeys;

import net.spy.memcached.MemcachedClientIF;

/**
 * This isn't very efficient right now.  It has to get/set a map of key sets
 * for each operation, which is not ideal.
 * 
 * @author avanderveen
 */
public class IndexedElastiCacheCCAdaptor extends ElastiCacheCCAdaptor {

	protected final TableKeys tables;

	public IndexedElastiCacheCCAdaptor(MemcachedClientIF cache) {
		super(cache);
		this.tables = new TableKeys(cache);
	}

	@Override
	public void put(String table, Object hashKey, Object rangeKey,
			String value, int ttl) {
		super.put(table, hashKey, rangeKey, value, ttl);
		String key = keyFor(table, hashKey, rangeKey);
		tables.add(table, key);
	}

	@Override
	public void remove(String table, Object hashKey, Object rangeKey) {
		super.remove(table, hashKey, rangeKey);
		String key = keyFor(table, hashKey, rangeKey);
		tables.remove(table, key);
	}

	@Override
	public void remove(String table) {
		//TODO bulk delete for keys in cache
		List<String> keys = tables.removeTable(table);
		for( String key : keys ) {
			cache.delete(key);
		}
	}

	@Override
	public void removeAll() {
		super.removeAll();
		tables.clear();
	}

	@Override
	public int count(String table) {
		return tables.count(table);
	}

}
