package com.dailycred.dynamonito.cache;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import com.dailycred.dynamonito.core.CacheAdaptor;
import com.dailycred.dynamonito.redis.RedisService;
import com.dailycred.dynamonito.redis.RedisService.RedisCallback;
import com.dailycred.dynamonito.redis.RedisService.RedisCallbackVoid;
import com.dailycred.dynamonito.redis.RedisService.RedisPipelinedCallback;

public class RedisCacheAdaptor implements CacheAdaptor {

  private final String delimiter;
  private final RedisService redis;
  private boolean enabled;

  public RedisCacheAdaptor(String delimiter, JedisPool jedisPool) {
    if (delimiter == null || delimiter.isEmpty())
      throw new IllegalArgumentException("The delimiter cannot be empty");
    this.delimiter = delimiter;
    redis = new RedisService(jedisPool);
    enabled = true;
  }

  @Override
  public void put(String table, final Object hashKey, Object rangeKey, final String value, final int expiration) {
    if (!enabled)
      return;
    final String redisKey = key(table, hashKey, rangeKey);
    if (expiration > 0) {
      redis.withRedisPipeLine(new RedisPipelinedCallback() {

        public void call(Pipeline pl) {
          pl.set(redisKey, value);
          pl.expire(redisKey, expiration);
        }
      });
    } else {
      redis.withRedis(new RedisCallbackVoid() {

        public void call(Jedis rd) {
          rd.set(redisKey, value);
        }
      });
    }
  }

  @Override
  public String get(String table, final Object hashKey, Object rangeKey) {
    if (!enabled)
      return null;
    final String redisKey = key(table, hashKey, rangeKey);
    return redis.withRedis(new RedisCallback<String>() {

      public String call(Jedis rd) {
        return rd.get(redisKey);
      }
    });
  }

  @Override
  public void remove(String table, final Object hashKey, Object rangeKey) {
    if (!enabled)
      return;
    final String redisKey = key(table, hashKey, rangeKey);
    redis.withRedis(new RedisCallbackVoid() {
      public void call(Jedis rd) {
        rd.del(redisKey);
      }
    });
  }

  @Override
  public void remove(final String table) {
    if (!enabled)
      return;
    // Can't use callback here because the outer var keys needs to be written.
    final Set<String> keys;
    Jedis rd = null;
    try {
      rd = redis.getPool().getResource();
      keys = rd.keys(table + "*");
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (rd != null)
        redis.getPool().returnResource(rd);
    }
    if (keys != null) {
      redis.withRedisPipeLine(new RedisPipelinedCallback() {

        @Override
        public void call(Pipeline pl) {
          pl.del(keys.toArray(new String[keys.size()]));
        }
      });
    }
  }

  @Override
  public void removeAll() {
    if (!enabled)
      return;
    redis.withRedis(new RedisCallbackVoid() {
      public void call(Jedis rd) {
        rd.flushDB();
      }
    });
  }

  @Override
  public int count(final String table) {
    if (!enabled)
      return -1;
    return redis.withRedis(new RedisCallback<Integer>() {

      public Integer call(Jedis rd) {
        Set<String> keys = rd.keys(table + "*");
        return keys.size();
      }
    });
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void setEnabled(boolean enableCache) {
    this.enabled = enableCache;
  }

  private String key(String table, Object hashKey, Object rangeKey) {
    if (hashKey.toString().contains(delimiter) || rangeKey.toString().contains(delimiter))
      throw new IllegalArgumentException("Key values cannot contain the delimiter.");
    return rangeKey == null ? table + delimiter + hashKey : table + delimiter + hashKey + delimiter + rangeKey;
  }
}
