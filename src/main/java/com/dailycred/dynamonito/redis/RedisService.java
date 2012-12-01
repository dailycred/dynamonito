package com.dailycred.dynamonito.redis;

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import com.amazonaws.services.dynamodb.model.AttributeValue;

/**
 * Convenience class for interacting with Redis. Clients don't have to worry about closing connections.
 * 
 */
public class RedisService {
  private JedisPool redisPool;

  public RedisService(JedisPool redisPool) {
    this.redisPool = redisPool;
  }

  /**
   * Perform a single operation with redis.
   * 
   * @param redisCallback
   * @return
   */
  public <T> T withRedis(RedisCallback<T> redisCallback) {
    Jedis rd = null;
    T result = null;
    try {
      rd = redisPool.getResource();
      result = redisCallback.call(rd);
    } finally {
      if (rd != null)
        redisPool.returnResource(rd);
    }
    return result;
  }

  /**
   * Perform a single operation with redis.
   * 
   * @param redisCallback
   */
  public void withRedis(RedisCallbackVoid redisCallback) {
    Jedis rd = null;
    try {
      rd = redisPool.getResource();
      redisCallback.call(rd);
    } finally {
      if (rd != null)
        redisPool.returnResource(rd);
    }
  }

  /**
   * Perform a batch operations with redis.
   * 
   * @param redisPipelinedCallback
   */
  public void withRedisPipeLine(RedisPipelinedCallback redisPipelinedCallback) {
    Jedis rd = null;
    Pipeline pipelined = null;
    try {
      rd = redisPool.getResource();
      pipelined = rd.pipelined();
      redisPipelinedCallback.call(pipelined);
      pipelined.sync();
    } finally {
      if (rd != null)
        redisPool.returnResource(rd);
    }
  }

  /**
   * Perform a transaction by applying a check-and-set on a key. If the key was modified during the transaction, the
   * entire transaction will be rolled back. Refer to <a href="http://redis.io/topics/transactions">Redis
   * Transactions</a> for details.
   * 
   * @param callback
   * @param keyToWatch
   *          the key to watch
   * @return
   */
  public Map<String, AttributeValue> withRedisCas(RedisCasCallback callback, String keyToWatch) {
    Jedis rd = null;
    Jedis rdTxn = null;
    Map<String, AttributeValue> attributes = null;
    try {
      rd = redisPool.getResource();
      rdTxn = redisPool.getResource();
      rdTxn.watch(keyToWatch);
      Transaction txn = rdTxn.multi();
      attributes = callback.call(txn, rd);
      List<Object> exec = txn.exec();
      if (exec == null) {
        throw new RuntimeException("Key " + keyToWatch + " was modified during CAS operation.");
      } else {
        return attributes;
      }
    } finally {
      if (rdTxn != null)
        redisPool.returnResource(rdTxn);
      if (rd != null)
        redisPool.returnResource(rd);
    }
  }

  public JedisPool getPool() {
    return redisPool;
  }

  public static interface RedisCallback<T> {
    T call(Jedis rd);
  }

  public static interface RedisCallbackVoid {
    void call(Jedis rd);
  }

  public static interface RedisPipelinedCallback {
    void call(Pipeline pl);
  }

  public static interface RedisCasCallback {
    /**
     * Perform a series on writes inside a transaction. Writes by <code>rd</code> are outside the scope of the
     * transaction.
     * 
     * @param txn
     *          the Redis transaction for writes only
     * @param rd
     *          the Redis connection for reads
     * @return a map of existing attributes for the cached item. Never null.
     */
    Map<String, AttributeValue> call(Transaction txn, Jedis rd);
  }

}