**Dynamonito is a drop in replacement for the [DynamoDB][1]'s high level mapper**. It intercepts the DynamoDB save operations, serializes the object into DynamoDB’s native wire protocol format in json, and puts the json in cache. The cache is a [write-through cache][2].

There are two pluggable cache implementations: in-memory and Redis. The in-memory cache is backed by Guava’s cache which is in-process and is extremely fast. The cache cannot be shared among multiple application hosts, so its use is limited to tests. The Redis cache resides on a Redis server and is out-of-process and so it can be accessed by many hosts. Optionally, you can set a time-to-live duration for cached objects.

Currently, Dynamonito can only cache objects that are mapped through the Java SDK, not through low level GetItem requests. It does not cache objects returned by the scan operation or the query operation.

### Getting Started
```java
AmazonDynamoDB client = new AmazonDynamoDBClient();

// In-memory cache
CacheAdaptor cacheAdaptorMemory = new InMemoryCacheAdaptor();

// Redis cache
CacheAdaptor cacheAdaptorRedis = 
  new RedisCacheAdaptor(",", new JedisPool("redis.example.com"));

// Construct the Dynamonito mapper
DynamonitoMapper cachedMapper = 
  new DynamonitoMapper(client, cacheAdaptorMemory);

// The Dynamonito mapper is API compatible with the standard DynamoDBMapper.
// Dynamonito only intercepts load(), save() and delete() operations.
// Rest of operations are delegated to DynamoDBMapper transparently.
Foo foo = cachedMapper.load(Foo.class, "ce6f2b8c-32e9-4884-9f22-ba95144038b8");
```

### License
Dynamonito is licensed under the Apache 2.0 license.

### Author
Shaun Ma
+ https://github.com/shaunma

 [1]: http://aws.amazon.com/dynamodb/
 [2]: http://en.wikipedia.org/wiki/Cache_(computing)#Writing_policies
