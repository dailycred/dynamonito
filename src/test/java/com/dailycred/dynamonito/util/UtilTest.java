package com.dailycred.dynamonito.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class UtilTest {

  @Test
  public void testGetJsonObjFromAttributeValue() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetKeyValueFromAttributeValue() {
    fail("Not yet implemented");
  }

  @Test
  public void testByteBufferToString() {
    fail("Not yet implemented");
  }

  @Test
  public void testParseItemJson() {
    fail("Not yet implemented");
  }

  @Test
  public void testToRedisKey() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetAttributeValue() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetStringFromAttributeValues() throws Exception {
    Map<String, AttributeValue> existingVals = null;
    Map<String, AttributeValueUpdate> newVals = null;
    Tuple2<Map<String, AttributeValue>, Set<String>> vals = null;
    {
      // Just add a new number field and set it to 1
      existingVals = ImmutableMap.of();
      newVals = ImmutableMap.of("f1", new AttributeValueUpdate(new AttributeValue().withN("1"), AttributeAction.ADD));
      vals = Util.applyUpdatedAttributeValues(existingVals, newVals);
      assertEquals(ImmutableMap.of("f1", new AttributeValue().withN("1")), vals._1);
    }
    {
      // Some floating point calcs
      // 1.23456789 - 0.987654321 = 0.246913569
      existingVals = ImmutableMap.of("f1", new AttributeValue().withN("1.23456789"));
      newVals = ImmutableMap.of("f1", new AttributeValueUpdate(new AttributeValue().withN("-0.987654321"),
          AttributeAction.ADD));
      vals = Util.applyUpdatedAttributeValues(existingVals, newVals);
      assertEquals(ImmutableMap.of("f1", new AttributeValue().withN("0.246913569")), vals._1);
    }
    {
      // Add an attribute to a string set
      existingVals = ImmutableMap.of("f1", new AttributeValue().withSS("foo", "bar"));
      newVals = ImmutableMap.of("f1", new AttributeValueUpdate(new AttributeValue().withS("baz"), AttributeAction.ADD));
      vals = Util.applyUpdatedAttributeValues(existingVals, newVals);
      assertEquals(ImmutableMap.of("f1", new AttributeValue().withSS("foo", "bar", "baz")), vals._1);
    }
    {
      // Remove a number set from a number set
      existingVals = ImmutableMap.of("f1", new AttributeValue().withNS("1.2345", "2"));
      newVals = ImmutableMap.of("f1", new AttributeValueUpdate(new AttributeValue().withNS("1.2345"),
          AttributeAction.DELETE));
      vals = Util.applyUpdatedAttributeValues(existingVals, newVals);
      assertEquals(ImmutableMap.of("f1", new AttributeValue().withNS("2")), vals._1);
    }
    {
      // Remove one number from a number set
      existingVals = ImmutableMap.of("f1", new AttributeValue().withNS("1.2345", "2"));
      newVals = ImmutableMap.of("f1", new AttributeValueUpdate(new AttributeValue().withN("1.2345"),
          AttributeAction.DELETE));
      vals = Util.applyUpdatedAttributeValues(existingVals, newVals);
      assertEquals(ImmutableMap.of(), vals._1);
      assertEquals(ImmutableSet.of("f1"), vals._2);
    }
    {
      // Add a number set to a number set
      existingVals = ImmutableMap.of("f1", new AttributeValue().withNS("1", "2"));
      newVals = ImmutableMap.of("f1", new AttributeValueUpdate(new AttributeValue().withNS("2", "3"),
          AttributeAction.ADD));
      vals = Util.applyUpdatedAttributeValues(existingVals, newVals);
      assertEquals(ImmutableMap.of("f1", new AttributeValue().withNS("3", "2", "1")), vals._1);
      assertEquals(ImmutableSet.of(), vals._2);
    }
    {
      // Remove all elements from a set
      existingVals = ImmutableMap.of("f1", new AttributeValue().withNS("1", "2"));
      newVals = ImmutableMap.of("f1", new AttributeValueUpdate(new AttributeValue().withNS("1", "2", "3"),
          AttributeAction.DELETE));
      vals = Util.applyUpdatedAttributeValues(existingVals, newVals);
      assertEquals(ImmutableMap.of(), vals._1);
      assertEquals(ImmutableSet.of("f1"), vals._2);
    }
    {
      // Remove an attribute
      existingVals = ImmutableMap.of("f1", new AttributeValue("bar"));
      newVals = ImmutableMap.of("f1", new AttributeValueUpdate(null, AttributeAction.DELETE));
      vals = Util.applyUpdatedAttributeValues(existingVals, newVals);
      assertEquals(ImmutableMap.of(), vals._1);
      assertEquals(ImmutableSet.of("f1"), vals._2);
    }
  }

}
