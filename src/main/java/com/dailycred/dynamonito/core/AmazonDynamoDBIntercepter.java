package com.dailycred.dynamonito.core;

import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodb.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.CreateTableResult;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableResult;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableResult;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.ListTablesRequest;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.amazonaws.services.dynamodb.model.UpdateTableRequest;
import com.amazonaws.services.dynamodb.model.UpdateTableResult;

import com.dailycred.dynamonito.util.Util;

/**
 * A partial implementation of {@link AmazonDynamoDB}. This intercepter intecepts writes from the high level mapper and
 * captures the JSON used in DynamoDB's wire transfer protocol.
 * 
 */
public class AmazonDynamoDBIntercepter implements AmazonDynamoDB {

  public ObjectNode cachedAttributes = null;

  public void setEndpoint(String endpoint) throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  public ListTablesResult listTables(ListTablesRequest listTablesRequest) throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public QueryResult query(QueryRequest queryRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest)
      throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) throws AmazonServiceException,
      AmazonClientException {
    cachedAttributes = JsonNodeFactory.instance.objectNode();
    /**
     * We are done with keys. Let's add the attributes.
     */
    Map<String, AttributeValueUpdate> attributeUpdates = updateItemRequest.getAttributeUpdates();
    for (Entry<String, AttributeValueUpdate> en : attributeUpdates.entrySet()) {
      String action = en.getValue().getAction();
      if (action.equals("PUT")) {
        AttributeValue av = en.getValue().getValue();
        cachedAttributes.put(en.getKey(), Util.getJsonObjFromAttributeValue(av));
      }
    }
    return null;
  }

  public PutItemResult putItem(PutItemRequest putItemRequest) throws AmazonServiceException, AmazonClientException {
    cachedAttributes = JsonNodeFactory.instance.objectNode();
    Map<String, AttributeValue> item = putItemRequest.getItem();
    for (Entry<String, AttributeValue> en : item.entrySet()) {
      AttributeValue av = en.getValue();
      cachedAttributes.put(en.getKey(), Util.getJsonObjFromAttributeValue(av));
    }
    return null;
  }

  public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public ScanResult scan(ScanRequest scanRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public CreateTableResult createTable(CreateTableRequest createTableRequest) throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public GetItemResult getItem(GetItemRequest getItemRequest) throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public ListTablesResult listTables() throws AmazonServiceException, AmazonClientException {
    throw new UnsupportedOperationException();
  }

  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    throw new UnsupportedOperationException();
  }

}
