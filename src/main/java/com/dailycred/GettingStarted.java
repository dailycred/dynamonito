package com.dailycred;

import redis.clients.jedis.JedisPool;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.dailycred.dynamonito.cache.*;
import com.dailycred.dynamonito.core.*;

public class GettingStarted {
  public static void main(String[] args) {
    AmazonDynamoDB client = BaseTest.getClient();

    // In-memory cache
    CacheAdaptor cacheAdaptorMemory = new InMemoryCacheAdaptor();

    // Redis cache
    CacheAdaptor cacheAdaptorRedis = new RedisCacheAdaptor(",", new JedisPool("redis.example.com"));

    // Construct the Dynamonito mapper
    DynamonitoMapper cachedMapper = new DynamonitoMapper(client, cacheAdaptorMemory);

    // The Dynamonito mapper is API compatible with the standard DynamoDBMapper.
    // Dynamonito only intercepts load(), save() and delete() operations. Rest of operations are delegated to DynamoDBMapper
    // transparently.
    cachedMapper.load(Object.class, "ce6f2b8c-32e9-4884-9f22-ba95144038b8");
  }
}
