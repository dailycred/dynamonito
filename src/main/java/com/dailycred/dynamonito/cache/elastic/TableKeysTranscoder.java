package com.dailycred.dynamonito.cache.elastic;

import java.util.Map;
import java.util.Set;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

public class TableKeysTranscoder extends BaseSerializingTranscoder
		implements Transcoder<Map<String,Set<String>>> {

	private static final int FLAGS = 0;

	public TableKeysTranscoder() {
		super(CachedData.MAX_SIZE);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Set<String>> decode(CachedData data) {
		return (Map<String, Set<String>>)deserialize(data.getData());
	}

	@Override
	public CachedData encode(Map<String, Set<String>> map) {
		byte[] data = serialize(map);
		return new CachedData(FLAGS, data, data.length);
	}

}
