package com.amazonaws.services.dynamodb.datamodelingv2;

import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
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
import com.dailycred.dynamonito.core.CacheAdaptor;

public class CachedAmazonDynamoDB implements AmazonDynamoDB {

	protected final AmazonDynamoDB delegate;
	protected final CacheAdaptor cache;

	protected final Map<String, Class<?>> tableMap;
	protected final DynamoDBMapper dynamoDummyMapper;

	public CachedAmazonDynamoDB(AmazonDynamoDB delegate, CacheAdaptor cache) {
		this.delegate = delegate;
		this.cache = cache;

		this.dynamoDummyMapper = new DynamoDBMapper(null);
		this.tableMap = null; //TODO
	}

	@Override
	public DeleteItemResult deleteItem(DeleteItemRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		cache.remove(arg0.getTableName(), arg0.getKey().getHashKeyElement().getS(), arg0.getKey().getRangeKeyElement().getS());
		return delegate.deleteItem(arg0);
	}

	@Override
	public DeleteTableResult deleteTable(DeleteTableRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		cache.remove(arg0.getTableName());
		return delegate.deleteTable(arg0);
	}

	@Override
	public GetItemResult getItem(GetItemRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		String cResult = cache.get(arg0.getTableName(), arg0.getKey().getHashKeyElement().getS(),
				arg0.getKey().getRangeKeyElement().getS());
		if( cResult == null ) {
			return delegate.getItem(arg0);
		}

		throw new UnsupportedOperationException();
	}

	@Override
	public PutItemResult putItem(PutItemRequest arg0)
			throws AmazonServiceException, AmazonClientException {
//		Class<?> clazz = tableMap.get(arg0.getTableName());
//		Object object = dynamoDummyMapper.marshallIntoObject(clazz, arg0.getItem());
//		ByteArrayOutputStream bos = new ByteArrayOutputStream();
//		String serialized = null;
//
//		try {
//			ObjectOutputStream oos = new ObjectOutputStream(bos);
//			oos.writeObject(object);
//			serialized = bos.toString("UTF-8");
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}

		throw new UnsupportedOperationException();
//		cache.put(arg0.getTableName(), null, null, serialized, -100);
//		return delegate.putItem(arg0);
	}

	@Override
	public UpdateItemResult updateItem(UpdateItemRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
//		return delegate.updateItem(arg0);
	}

	@Override
	public UpdateTableResult updateTable(UpdateTableRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
//		return delegate.updateTable(arg0);
	}

	// no-cache from here down
	@Override
	public BatchGetItemResult batchGetItem(BatchGetItemRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		return delegate.batchGetItem(arg0);
	}

	@Override
	public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		return delegate.batchWriteItem(arg0);
	}

	@Override
	public CreateTableResult createTable(CreateTableRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		return delegate.createTable(arg0);
	}

	@Override
	public DescribeTableResult describeTable(DescribeTableRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		return delegate.describeTable(arg0);
	}

	@Override
	public ResponseMetadata getCachedResponseMetadata(
			AmazonWebServiceRequest arg0) {
		return delegate.getCachedResponseMetadata(arg0);
	}

	@Override
	public ListTablesResult listTables() throws AmazonServiceException,
			AmazonClientException {
		return delegate.listTables();
	}

	@Override
	public ListTablesResult listTables(ListTablesRequest arg0)
			throws AmazonServiceException, AmazonClientException {
		return delegate.listTables(arg0);
	}

	@Override
	public QueryResult query(QueryRequest arg0) throws AmazonServiceException,
			AmazonClientException {
		return delegate.query(arg0);
	}

	@Override
	public ScanResult scan(ScanRequest arg0) throws AmazonServiceException,
			AmazonClientException {
		return delegate.scan(arg0);
	}

	@Override
	public void setEndpoint(String arg0) throws IllegalArgumentException {
		delegate.setEndpoint(arg0);
	}

	@Override
	public void shutdown() {
		delegate.shutdown();
	}

}
