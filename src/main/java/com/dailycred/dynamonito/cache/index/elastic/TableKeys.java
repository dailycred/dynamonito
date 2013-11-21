package com.dailycred.dynamonito.cache.index.elastic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.spy.memcached.CASMutation;
import net.spy.memcached.CASMutator;
import net.spy.memcached.MemcachedClientIF;

public class TableKeys {

	public static final String DEFAULT_TABLE_INDEX_KEY = "dynamonito-elasticacheclusterclient-adapter-table-index";
	private static final int EXPIRATION = 0; // never expire

	protected final MemcachedClientIF cache;
	protected String key;

	protected TableKeysTranscoder transcoder;
	protected CASMutator<Map<String,List<String>>> mutator;
	protected Map<String,List<String>> initialValue;


	public TableKeys(MemcachedClientIF cache) {
		this(cache, DEFAULT_TABLE_INDEX_KEY);
	}

	public TableKeys(MemcachedClientIF cache, String key) {
		this.cache = cache;
		this.key = key;
		this.transcoder = new TableKeysTranscoder();
		this.mutator = new CASMutator<Map<String,List<String>>>(cache, transcoder);
		this.initialValue = new HashMap<String,List<String>>();
	}


	public Map<String,List<String>> add(final String table, final String key) {
		return mutate(new CASMutation<Map<String,List<String>>>() {
			@Override
			public Map<String, List<String>> getNewValue(
					Map<String, List<String>> tableKeys) {
				tableKeys.get(table).add(key);
				return tableKeys;
			}
		});
	}

	public Map<String,List<String>> remove(final String table, final String key) {
		return mutate(new CASMutation<Map<String,List<String>>>() {
			@Override
			public Map<String, List<String>> getNewValue(
					Map<String, List<String>> tableKeys) {
				tableKeys.get(table).remove(key);
				return tableKeys;
			}
		});
	}

	@SuppressWarnings("unchecked")
	public List<String> removeTable(final String table) {
		List<String> keys = ((Map<String, List<String>>)cache.get(key)).get(table);
		mutate(new CASMutation<Map<String,List<String>>>() {
			@Override
			public Map<String, List<String>> getNewValue(
					Map<String, List<String>> tableKeys) {
				tableKeys.remove(table);
				return tableKeys;
			}
		});
		return keys;
	}

	@SuppressWarnings("unchecked")
	public int count(String table) {
		List<String> keys = ((Map<String, List<String>>)cache.get(key)).get(table);
		return (keys == null ? 0 : keys.size());
	}

	public void clear() {
		cache.delete(key);
	}

	private Map<String,List<String>> mutate(CASMutation<Map<String,List<String>>> mutation) {
		try {
			return mutator.cas(this.key, initialValue, EXPIRATION, mutation);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
