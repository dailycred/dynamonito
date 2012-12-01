package com.dailycred.dynamonito.core;

import com.amazonaws.services.dynamodb.model.AttributeValue;

/**
 * A container object that represents a unique DynamoDB item.
 * 
 */
public class ModelMetadata {

  public final String tableName;
  public final String hashKeyName;
  public final String rangeKeyName;
  public final AttributeValue hashKeyValue;
  public final AttributeValue rangeKeyValue;

  public ModelMetadata(String tableName, String hashKeyName, String rangeKeyName, AttributeValue hashKeyValue, AttributeValue rangeKeyValue) {
    this.tableName = tableName;
    this.hashKeyName = hashKeyName;
    this.rangeKeyName = rangeKeyName;
    this.hashKeyValue = hashKeyValue;
    this.rangeKeyValue = rangeKeyValue;
  }
}
