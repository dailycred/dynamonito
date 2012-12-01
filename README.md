At Dailycred we use DynamoDB as a data store, and there's a lot to like about it:

*  scalability and simplicity of NoSQL
*  consistent performance
*  low learning curve
*  it comes with a decent object mapper for Java

It’s a great service considering how new it is, but it definitely still has some rough edges that make some things harder than we expected.

![](http://media.tumblr.com/tumblr_me7sysGcEe1rrhf18.png)
### DynamoDB's not ideal for storing events

Like most websites, we store a variety of user events. A typical event has an event ID, a user ID, an event type and other attributes that describe actions performed by users. At Dailycred, **we needed an event storage that is optimized for reads**. For our dashboard we need to quickly filter events by type, sort events by time and group events by user. However, we don’t need to record events as soon as they happen. A second of delay is fine.

Using a relational database, we can store events in a denormalized table. Add indices to columns that answer query predicates. In this setup, writes are not very fast, but reads are extremely fast. We can use the richness of SQL to query events easily.

A big limitation of DynamoDB and other non-relational database is the lack of multiple indices. In an events table, we could use the event ID as the hash key, the event time as the range key. This schema would enable us to retrieve most recent events, but we **can’t filter event by type without doing a full table scan**. Scans are expensive in DynamoDB. You could store events in many tables, partitioned by event type or by user ID. Perhaps for each predicate, create an index table with the predicate’s value as the key and the event ID as an attribute. Is the added complexity worth it? Probably not. DynamoDB is great for lookups by key, not so good for queries, and abysmal for queries with multiple predicates.

SQL was a better tool in this case, so we decided not to use DynamoDB at all for storing events.

### DynamoDB overhead (compared to SQL)

DynamoDB supports transactions, but not in the traditional SQL sense. Each write operation is atomic to an item. A write operation either successfully updates all of the item’s attributes or none of its attributes. **There are no multi-operation transactions**. For example, you have two tables, one to store orders and one to store the user-to-order mapping. When a new order comes in, you write to the order table first, then the mapping table. If the second write fails due to network outage, **you are left with an orphaned item**. Your application has to recognize orphaned data. Periodically, you will want to run a script to garbage collect those data, which in turn involve a full table scan. The complexity doesn’t end here. Your script might need to increase the read limit temporarily. It has to wait long enough between rounds of scan to stay under the limit.

### One strike, and you are out

While DynamoDB’s provisioned throughput lets you fine tune the performance of individual tables, it doesn't degrade gracefully. **Once you hit the read or write limit, your requests are denied** until enough time has elapsed. In a perfect world, your auto-scaling script will adjust throughput based on anticipated traffic, increasing and decreasing limits as necessary, but unexpected traffic spikes is a fact of life. Say you bump up the limits as soon as DynamoDB throws a ProvisionedThroughputExceededException, the process could take a minute to complete. Until then, you are at the mercy of retries, a feature that is thankfully enabled by default by the official SDK.

At Dailycred, it’s hard to accurately anticipate resource usage, due to the fact that we do not know when our clients are hit with a wave of visitors. What if the wave happens to multiple clients at once?

### Backups: slow or expensive (pick one)

Another annoyance with provisioned throughput is that you **can only decrease your provisioned limit once a day**. Say your daily backup script temporarily increases the read limits for the duration of the job. After the limits were reduced, your site gets a burst of traffic. After increasing the limits for the second time, you are stuck with the new limits for up to a day. In that regard, DynamoDB isn’t as elastic as other AWS services, where resources can be deallocated with no daily limits.

### Unit tests: slow or expensive (pick one)

We also run a lot of tests that use DynamoDB, which means a lot of items are written and read very quickly. We run the tests several times a day, which means our development database tables are sitting completely idle most of the time, only to be hammered with reads and writes when we run our unit tests. From a cost perspective, this isn't ideal. However, it's even worse if a developer has to wait extra time for his unit tests to complete.

### Our solution: Dynamonito, a lightweight caching utility for DynamoDB
![](http://media.tumblr.com/tumblr_me7rlbEic01rrhf18.png)
To work around some of these limitations, we built Dynamonito. Dynamonito is an open source tool we built to cache DynamoDB data, and save us an unnecessarily large bill.

**Dynamonito is a drop in replacement for the high level mapper**. It intercepts the DynamoDB save operations, serializes the object into DynamoDB’s native wire protocol format in json, and puts the json in cache. The cache is a [write-through cache][3].

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


### Author
Shaun Ma
+ https://github.com/shaunma

 [3]: http://en.wikipedia.org/wiki/Cache_(computing)#Writing_policies
 [4]: https://github.com/dailycred/dynamonito