package com.dailycred.dynamonito.cache.elastic;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.spy.memcached.CASMutation;
import net.spy.memcached.CASMutator;
import net.spy.memcached.MemcachedClientIF;

public class TableKeys {

	public static final String DEFAULT_TABLE_INDEX_KEY = "dynamonito-elasticacheclusterclient-adapter-table-index";
	private static final int EXPIRATION = 0; // never expire

	protected final MemcachedClientIF cache;
	protected String key;

	protected TableKeysTranscoder transcoder;
	protected CASMutator<Map<String,Set<String>>> mutator;
	protected Map<String,Set<String>> initialValue;


	public TableKeys(MemcachedClientIF cache) {
		this(cache, DEFAULT_TABLE_INDEX_KEY);
	}

	public TableKeys(MemcachedClientIF cache, String key) {
		this.cache = cache;
		this.key = key;
		this.transcoder = new TableKeysTranscoder();
		this.mutator = new CASMutator<Map<String,Set<String>>>(cache, transcoder);
		this.initialValue = new HashMap<String,Set<String>>();
	}


	public Map<String,Set<String>> add(final String table, final String key) {
		return mutate(new CASMutation<Map<String,Set<String>>>() {
			@Override
			public Map<String, Set<String>> getNewValue(
					Map<String, Set<String>> tableKeys) {
				tableKeys.get(table).add(key);
				return tableKeys;
			}
		});
	}

	public Map<String,Set<String>> remove(final String table, final String key) {
		return mutate(new CASMutation<Map<String,Set<String>>>() {
			@Override
			public Map<String, Set<String>> getNewValue(
					Map<String, Set<String>> tableKeys) {
				tableKeys.get(table).remove(key);
				return tableKeys;
			}
		});
	}

	@SuppressWarnings("unchecked")
	public Set<String> removeTable(final String table) {
		Set<String> keys = ((Map<String, Set<String>>)cache.get(key)).get(table);
		mutate(new CASMutation<Map<String,Set<String>>>() {
			@Override
			public Map<String, Set<String>> getNewValue(
					Map<String, Set<String>> tableKeys) {
				tableKeys.remove(table);
				return tableKeys;
			}
		});
		return keys;
	}

	@SuppressWarnings("unchecked")
	public int count(String table) {
		Set<String> keys = ((Map<String, Set<String>>)cache.get(key)).get(table);
		return (keys == null ? 0 : keys.size());
	}

	public void clear() {
		cache.delete(key);
	}

	private Map<String,Set<String>> mutate(CASMutation<Map<String,Set<String>>> mutation) {
		try {
			return mutator.cas(this.key, initialValue, EXPIRATION, mutation);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
