package com.dailycred.dynamonito.core;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;
import com.dailycred.dynamonito.cache.InMemoryCacheAdaptor;
import com.dailycred.dynamonito.util.Util;
import com.google.common.base.Objects;

public class HashRangeOnlyTest extends BaseTest {

  @DynamoDBCache
  @DynamoDBTable(tableName = "overwritten_in_config")
  public static class HashRangeOnly {
    private String hash;
    private String range;

    public HashRangeOnly() {

    }

    public HashRangeOnly(String hash, String range) {
      super();
      this.hash = hash;
      this.range = range;
    }

    @DynamoDBHashKey
    public String getHash() {
      return hash;
    }

    public void setHash(String hash) {
      this.hash = hash;
    }

    @DynamoDBRangeKey
    public String getRange() {
      return range;
    }

    public void setRange(String range) {
      this.range = range;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(hash, range);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      HashRangeOnly that = (HashRangeOnly) obj;
      return Objects.equal(hash, that.hash) && Objects.equal(range, that.range);
    }
  }

  @Before
  public void before() {
  }

  @Test
  public void testKeyOnlyPut() throws Exception {
    DynamoDBMapperConfig config = new DynamoDBMapperConfig(new DynamoDBMapperConfig.TableNameOverride(
        "hash_range_keys_only"));
    getDynamoDBMapper().delete(new HashRangeOnly("hash", "range"), config);
    InMemoryCacheAdaptor cacheAdaptor = new InMemoryCacheAdaptor();
    DynamonitoMapper mapper = new DynamonitoMapper(getClient(), config, cacheAdaptor);
    HashRangeOnly obj = new HashRangeOnly("hash", "range");
    mapper.save(obj, config);
    String json = cacheAdaptor.get("hash_range_keys_only", obj.getHash(), obj.getRange());
    HashRangeOnly cachedObj = mapper.marshallIntoObject(HashRangeOnly.class,
        Util.parseItemJson(new ObjectMapper().readTree(json)));
    Assert.assertEquals(obj, cachedObj);
  }
}
