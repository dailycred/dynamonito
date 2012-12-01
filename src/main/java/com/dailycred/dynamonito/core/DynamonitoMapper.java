package com.dailycred.dynamonito.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMappingException;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodb.datamodeling.DynamonitoReflector;
import com.amazonaws.services.dynamodb.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodb.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;

import com.dailycred.dynamonito.util.Util;

public class DynamonitoMapper {

  private final DynamoDBMapperConfig config;
  private final DynamoDBMapper mapper;
  private final DynamonitoReflector reflector;
  private final CacheAdaptor cacheAdaptor;
  private final AmazonDynamoDBIntercepter intercepter = new AmazonDynamoDBIntercepter();

  /**
   * Create a DynamonitoMapper with default DynamoDB mapper config.
   * 
   * @param client
   * @param cacheAdaptor
   */
  public DynamonitoMapper(AmazonDynamoDB client, CacheAdaptor cacheAdaptor) {
    this(client, DynamoDBMapperConfig.DEFAULT, cacheAdaptor);
  }

  /**
   * Create a DynamonitoMapper with custon DynamoDB mapper config.
   * 
   * @param client
   * @param config
   * @param cachaeAdaptor
   */
  public DynamonitoMapper(AmazonDynamoDB client, DynamoDBMapperConfig config, CacheAdaptor cachaeAdaptor) {
    this.mapper = new DynamoDBMapper(client);
    this.config = config;
    this.cacheAdaptor = cachaeAdaptor;
    this.reflector = new DynamonitoReflector(mapper);
  }

  public <T extends Object> T load(Class<T> clazz, Object hashKey, DynamoDBMapperConfig config) {
    return load(clazz, hashKey, null, config);
  }

  /**
   * Loads an object with the hash key given, using the default configuration.
   * 
   * @see DynamoDBMapper#load(Class, Object, Object, DynamoDBMapperConfig)
   */
  public <T extends Object> T load(Class<T> clazz, Object hashKey) {
    return load(clazz, hashKey, null, config);
  }

  /**
   * Loads an object with a hash and range key, using the default configuration.
   * 
   * @see DynamoDBMapper#load(Class, Object, Object, DynamoDBMapperConfig)
   */
  public <T extends Object> T load(Class<T> clazz, Object hashKey, Object rangeKey) {
    return load(clazz, hashKey, rangeKey, config);
  }

  /**
   * Returns an object with the given hash key, or null if no such object exists.
   * 
   * @param clazz
   *          The class to load, corresponding to a DynamoDB table.
   * @param hashKey
   *          The key of the object.
   * @param rangeKey
   *          The range key of the object, or null for tables without a range key.
   * @param config
   *          Configuration for the service call to retrieve the object from DynamoDB. This configuration overrides the
   *          default given at construction.
   */
  public <T extends Object> T load(Class<T> clazz, Object hashKey, Object rangeKey, DynamoDBMapperConfig config) {
    if (hashKey == null)
      throw new IllegalArgumentException("Hash key is null when loading class " + clazz.getCanonicalName());
    T obj = null;
    // Try cache first.
    try {
      String json = cacheAdaptor.get(getTableName(clazz, config), hashKey, rangeKey);
      if (json != null) {
        JsonNode itemJson = new ObjectMapper().readTree(json);
        Map<String, AttributeValue> item = null;
        item = Util.parseItemJson(itemJson);
        obj = mapper.marshallIntoObject(clazz, item);
      }
    } catch (Exception e) {
      throw new DynamonitoCacheException(e);
    }
    // If not in cache, try source.
    if (obj == null) {
      obj = mapper.load(clazz, hashKey, rangeKey, config);
      // If found in source, cache the found object.
      if (obj != null) {
        try {
          cacheObject(obj, config);
        } catch (Exception e) {
          throw new DynamonitoCacheException(e);
        }
      }
    }
    return obj;
  }

  /**
   * Creates and fills in the attributes on an instance of the class given with the attributes given.
   * <p>
   * This is accomplished by looking for getter methods annotated with an appropriate annotation, then looking for
   * matching attribute names in the item attribute map.
   * 
   * @param clazz
   *          The class to instantiate and hydrate
   * @param itemAttributes
   *          The set of item attributes, keyed by attribute name.
   */
  public <T> T marshallIntoObject(Class<T> clazz, Map<String, AttributeValue> itemAttributes) {
    return mapper.marshallIntoObject(clazz, itemAttributes);
  }

  /**
   * Marshalls the list of item attributes into objects of type clazz
   * 
   * @see DynamoDBMapper#marshallIntoObject(Class, Map)
   */
  public <T> List<T> marshallIntoObjects(Class<T> clazz, List<Map<String, AttributeValue>> itemAttributes) {
    return mapper.marshallIntoObjects(clazz, itemAttributes);
  }

  /**
   * Saves the object given into DynamoDB, using the default configuration.
   * 
   * @see DynamoDBMapper#save(Object, DynamoDBMapperConfig)
   */
  public <T extends Object> void save(T object) {
    mapper.save(object);
    cacheObject(object, DynamoDBMapperConfig.DEFAULT);
  }

  /**
   * Saves an item in DynamoDB. The service method used is determined by the
   * {@link DynamoDBMapperConfig#getSaveBehavior()} value, to use either {@link AWSDynamoDB#putItem(PutItemRequest)} or
   * {@link AWSDynamoDB#updateItem(UpdateItemRequest)}. For updates, a null value for an object property will remove it
   * from that item in DynamoDB. For puts, a null value will not be passed to the service. The effect is therefore the
   * same, except when the item in DynamoDB contains attributes that aren't modeled by the domain object given.
   * 
   * @param object
   *          The object to save into DynamoDB
   * @param config
   *          The configuration to use, which overrides the default provided at object construction.
   */
  public <T extends Object> void save(T object, DynamoDBMapperConfig config) {
    mapper.save(object, config);
    cacheObject(object, config);
  }

  /**
   * Deletes the given object from its DynamoDB table.
   */
  public void delete(Object object) {
    delete(object, this.config);
  }

  /**
   * Deletes the given object from its DynamoDB table.
   * 
   * @param config
   *          Config override object. If {@link SaveBehavior#CLOBBER} is supplied, version fields will not be considered
   *          when deleting the object.
   */
  public void delete(Object object, DynamoDBMapperConfig config) throws DynamonitoCacheException {
    mapper.delete(object, config);
    deleteCachedObject(object, config);
  }

  /**
   * Deletes the objects given using one or more calls to the
   * {@link AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)} API.
   * 
   * @see DynamoDBMapper#batchWrite(List, List, DynamoDBMapperConfig)
   */
  public void batchDelete(List<? extends Object> objectsToDelete) {
    mapper.batchDelete(objectsToDelete);
  }

  /**
   * Deletes the objects given using one or more calls to the
   * {@link AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)} API.
   * 
   * @see DynamoDBMapper#batchWrite(List, List, DynamoDBMapperConfig)
   */
  public void batchDelete(Object... objectsToDelete) {
    mapper.batchDelete(objectsToDelete);
  }

  /**
   * Saves the objects given using one or more calls to the {@link AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)}
   * API.
   * 
   * @see DynamoDBMapper#batchWrite(List, List, DynamoDBMapperConfig)
   */
  public void batchSave(List<? extends Object> objectsToSave) {
    mapper.batchSave(objectsToSave);
  }

  /**
   * Saves the objects given using one or more calls to the {@link AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)}
   * API.
   * 
   * @see DynamoDBMapper#batchWrite(List, List, DynamoDBMapperConfig)
   */
  public void batchSave(Object... objectsToSave) {
    mapper.batchSave(objectsToSave);
  }

  /**
   * Saves and deletes the objects given using one or more calls to the
   * {@link AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)} API.
   * 
   * @see DynamoDBMapper#batchWrite(List, List, DynamoDBMapperConfig)
   */
  public void batchWrite(List<? extends Object> objectsToWrite, List<? extends Object> objectsToDelete) {
    mapper.batchWrite(objectsToWrite, objectsToDelete);
  }

  /**
   * Saves and deletes the objects given using one or more calls to the
   * {@link AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)} API.
   * 
   * @param objectsToWrite
   *          A list of objects to save to DynamoDB. No version checks are performed, as required by the
   *          {@link AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)} API.
   * @param objectsToDelete
   *          A list of objects to delete from DynamoDB. No version checks are performed, as required by the
   *          {@link AmazonDynamoDB#batchWriteItem(BatchWriteItemRequest)} API.
   * @param config
   *          Only {@link DynamoDBMapperConfig#getTableNameOverride()} is considered; if specified, all objects in the
   *          two parameter lists will be considered to belong to the given table override.
   */
  public void batchWrite(List<? extends Object> objectsToWrite, List<? extends Object> objectsToDelete,
      DynamoDBMapperConfig config) {
    mapper.batchWrite(objectsToWrite, objectsToDelete, config);
  }

  /**
   * Scans through an AWS DynamoDB table and returns the matching results as an unmodifiable list of instantiated
   * objects, using the default configuration.
   * 
   * @see DynamoDBMapper#scan(Class, DynamoDBScanExpression, DynamoDBMapperConfig)
   */
  public <T> PaginatedScanList<T> scan(Class<T> clazz, DynamoDBScanExpression scanExpression) {
    return mapper.scan(clazz, scanExpression);
  }

  /**
   * Scans through an AWS DynamoDB table and returns the matching results as an unmodifiable list of instantiated
   * objects. The table to scan is determined by looking at the annotations on the specified class, which declares where
   * to store the object data in AWS DynamoDB, and the scan expression parameter allows the caller to filter results and
   * control how the scan is executed.
   * <p>
   * Callers should be aware that the returned list is unmodifiable, and any attempts to modify the list will result in
   * an UnsupportedOperationException.
   * <p>
   * The unmodifiable list returned is lazily loaded when possible, so calls to DynamoDB will be made only as needed.
   * 
   * @param <T>
   *          The type of the objects being returned.
   * @param clazz
   *          The class annotated with DynamoDB annotations describing how to store the object data in AWS DynamoDB.
   * @param scanExpression
   *          Details on how to run the scan, including any filters to apply to limit results.
   * @param config
   *          The configuration to use for this scan, which overrides the default provided at object construction.
   * @return An unmodifiable list of the objects constructed from the results of the scan operation.
   * @see PaginatedScanList
   */
  public <T> PaginatedScanList<T> scan(Class<T> clazz, DynamoDBScanExpression scanExpression,
      DynamoDBMapperConfig config) {
    return mapper.scan(clazz, scanExpression, config);
  }

  /**
   * Queries an AWS DynamoDB table and returns the matching results as an unmodifiable list of instantiated objects,
   * using the default configuration.
   * 
   * @see DynamoDBMapper#query(Class, DynamoDBQueryExpression, DynamoDBMapperConfig)
   */
  public <T> PaginatedQueryList<T> query(Class<T> clazz, DynamoDBQueryExpression queryExpression) {
    return mapper.query(clazz, queryExpression);
  }

  /**
   * Queries an AWS DynamoDB table and returns the matching results as an unmodifiable list of instantiated objects. The
   * table to query is determined by looking at the annotations on the specified class, which declares where to store
   * the object data in AWS DynamoDB, and the query expression parameter allows the caller to filter results and control
   * how the query is executed.
   * <p>
   * Callers should be aware that the returned list is unmodifiable, and any attempts to modify the list will result in
   * an UnsupportedOperationException.
   * <p>
   * The unmodifiable list returned is lazily loaded when possible, so calls to DynamoDB will be made only as needed.
   * 
   * @param <T>
   *          The type of the objects being returned.
   * @param clazz
   *          The class annotated with DynamoDB annotations describing how to store the object data in AWS DynamoDB.
   * @param queryExpression
   *          Details on how to run the query, including any filters to apply to limit the results.
   * @param config
   *          The configuration to use for this query, which overrides the default provided at object construction.
   * @return An unmodifiable list of the objects constructed from the results of the query operation.
   * 
   * @see PaginatedQueryList
   */
  public <T> PaginatedQueryList<T> query(Class<T> clazz, DynamoDBQueryExpression queryExpression,
      DynamoDBMapperConfig config) {
    return mapper.query(clazz, queryExpression, config);
  }

  /**
   * Evaluates the specified scan expression and returns the count of matching items, without returning any of the
   * actual item data, using the default configuration.
   * 
   * @see DynamoDBMapper#count(Class, DynamoDBScanExpression, DynamoDBMapperConfig)
   */
  public int count(Class<?> clazz, DynamoDBScanExpression scanExpression) {
    return count(clazz, scanExpression, config);
  }

  /**
   * Evaluates the specified scan expression and returns the count of matching items, without returning any of the
   * actual item data.
   * <p>
   * This operation will scan your entire table, and can therefore be very expensive. Use with caution.
   * 
   * @param clazz
   *          The class mapped to a DynamoDB table.
   * @param scanExpression
   *          The parameters for running the scan.
   * @param config
   *          The configuration to use for this scan, which overrides the default provided at object construction.
   * @return The count of matching items, without returning any of the actual item data.
   */
  public int count(Class<?> clazz, DynamoDBScanExpression scanExpression, DynamoDBMapperConfig config) {
    return mapper.count(clazz, scanExpression, config);
  }

  /**
   * Evaluates the specified query expression and returns the count of matching items, without returning any of the
   * actual item data, using the default configuration.
   * 
   * @see DynamoDBMapper#count(Class, DynamoDBQueryExpression, DynamoDBMapperConfig)
   */
  public int count(Class<?> clazz, DynamoDBQueryExpression queryExpression) {
    return count(clazz, queryExpression, config);
  }

  /**
   * Evaluates the specified query expression and returns the count of matching items, without returning any of the
   * actual item data.
   * 
   * @param clazz
   *          The class mapped to a DynamoDB table.
   * @param scanExpression
   *          The parameters for running the scan.
   * @param config
   *          The mapper configuration to use for the query, which overrides the default provided at object
   *          construction.
   * @return The count of matching items, without returning any of the actual item data.
   */
  public int count(Class<?> clazz, DynamoDBQueryExpression queryExpression, DynamoDBMapperConfig config) {
    return mapper.count(clazz, queryExpression, config);
  }

  /**
   * Swallows the checked exceptions around Method.invoke and repackages them
   * as {@link DynamoDBMappingException}
   */
  private Object safeInvoke(Method method, Object object, Object... arguments) {
      try {
          return method.invoke(object, arguments);
      } catch ( IllegalAccessException e ) {
          throw new DynamoDBMappingException("Couldn't invoke " + method, e);
      } catch ( IllegalArgumentException e ) {
          throw new DynamoDBMappingException("Couldn't invoke " + method, e);
      } catch ( InvocationTargetException e ) {
          throw new DynamoDBMappingException("Couldn't invoke " + method, e);
      }
  }
  
  public DynamoDBMapper getDynamoDBMapper() {
    return mapper;
  }

  /**
   * Start of reflection code
   */
  
  /**
   * Get the value of the hash key from the raw hash key value.
   * 
   * @param hashKey
   *          the raw hash key value.
   * @param hashKeyGetter
   *          the hash key's getter; used to determine the type of the key
   * @return
   */
  public AttributeValue getHashKeyElement(Object hashKey, Method hashKeyGetter) {
    try {
      Class<?>[] parameterTypes = new Class<?>[] { Object.class, Method.class };
      Object[] args = new Object[] { hashKey, hashKeyGetter };
      return invokePrivateMapperMethod("getHashKeyElement", parameterTypes, args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the range key from the raw range key value.
   * 
   * @param hashKey
   *          the raw range key value.
   * @param hashKeyGetter
   *          the range key's getter; used to determine the type of the key
   * @return
   */
  public AttributeValue getRangeKeyElement(Object rangeKey, Method rangeKeyGetter) {
    try {
      Class<?>[] parameterTypes = new Class<?>[] { Object.class, Method.class };
      Object[] args = new Object[] { rangeKey, rangeKeyGetter };
      return invokePrivateMapperMethod("getRangeKeyElement", parameterTypes, args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <T> String getTableName(Class<T> clazz, DynamoDBMapperConfig config) {
    try {
      Class<?>[] parameterTypes = new Class<?>[] { Class.class, DynamoDBMapperConfig.class };
      Object[] args = new Object[] { clazz, config };
      return invokePrivateMapperMethod("getTableName", parameterTypes, args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private DynamoDBMapperConfig mergeConfig(DynamoDBMapperConfig config) {
    try {
      Class<?>[] parameterTypes = new Class<?>[] { DynamoDBMapperConfig.class };
      Object[] args = new Object[] { config };
      return invokePrivateMapperMethod("mergeConfig", parameterTypes, args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * End of reflection code
   */

  /**
   * Cache a model object.
   * 
   * @param object
   * @throws DynamonitoCacheException
   */
  private <T> void cacheObject(T object, DynamoDBMapperConfig config) throws DynamonitoCacheException {
    Class<?> clazz = object.getClass();
    DynamoDBCache cacheAnno = clazz.getAnnotation(DynamoDBCache.class);
    if (cacheAnno == null)
      throw new DynamonitoCacheException("The object's class (\"" + clazz.getCanonicalName()
          + "\") you are trying to cache must be annotated with @DynamoDBCache. ");
    config = mergeConfig(config);
    try {
      DynamoDBMapper fakeMapper = new DynamoDBMapper(intercepter);
      fakeMapper.save(object);
      ObjectNode item = intercepter.cachedAttributes;
      ModelMetadata metadata = getModelMetadata(object, config);

      /**
       * The cached attributes doean't contain the keys, so we have to append the keys to the item manually.
       */
      item.put(metadata.hashKeyName, Util.getJsonObjFromAttributeValue(metadata.hashKeyValue));
      if (metadata.rangeKeyValue != null)
        item.put(metadata.rangeKeyName, Util.getJsonObjFromAttributeValue(metadata.rangeKeyValue));

      Object hashKeyValueObj = Util.getKeyValueFromAttributeValue(metadata.hashKeyValue);
      Object rangeKeyValueObj = metadata.rangeKeyValue == null ? null : Util
          .getKeyValueFromAttributeValue(metadata.rangeKeyValue);
      cacheAdaptor.put(metadata.tableName, hashKeyValueObj, rangeKeyValueObj, item.toString(), cacheAnno.ttl());
    } catch (Exception e) {
      throw new DynamonitoCacheException(e);
    }
  }

  /**
   * Delete an object from cache.
   * 
   * @param object
   * @param config
   * @throws DynamonitoCacheException
   */
  private void deleteCachedObject(Object object, DynamoDBMapperConfig config) throws DynamonitoCacheException {
    config = mergeConfig(config);
    try {
      ModelMetadata metadata = getModelMetadata(object, config);
      Object hashKeyValueObj = Util.getKeyValueFromAttributeValue(metadata.hashKeyValue);
      Object rangeKeyValueObj = metadata.rangeKeyValue == null ? null : Util.getKeyValueFromAttributeValue(metadata.rangeKeyValue);
      cacheAdaptor.remove(metadata.tableName, hashKeyValueObj, rangeKeyValueObj);
    } catch (Exception e) {
      throw new DynamonitoCacheException(e);
    }
  }

  /**
   * Utility to invoke a private method in {@link DynamoDBMapper}.
   * 
   * @param methodName
   *          the name of the method
   * @param parameterTypes
   *          parameter types
   * @param args
   *          arguments for the invocation
   * @return the invocation result
   * @throws SecurityException
   * @throws NoSuchMethodException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  @SuppressWarnings("unchecked")
  private <T> T invokePrivateMapperMethod(String methodName, Class<?>[] parameterTypes, Object[] args)
      throws SecurityException,
      NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    Method method = mapper.getClass().getDeclaredMethod(methodName, parameterTypes);
    method.setAccessible(true);
    return (T) method.invoke(mapper, args);
  }
  
  /**
   * Get the names of the hash key and the range key from a model class.
   * 
   * @param clazz
   * @return if model doesn't have a range key, return array of size 1 with the hash key name, else return array of size
   *         2 with hash and range key names.
   */
  private String[] getKeyNames(Class<?> clazz) {
    final String[] keyNames;
    Method hashKeyGetter = reflector.getHashKeyGetter(clazz);
    String hashKeyName = reflector.getAttributeName(hashKeyGetter);
    Method rangeKeyGetter = reflector.getRangeKeyGetter(clazz);
    if (rangeKeyGetter == null) {
      keyNames = new String[] { hashKeyName };
    } else {
      String rangeKeyName = reflector.getAttributeName(rangeKeyGetter);
      keyNames = new String[] { hashKeyName, rangeKeyName };
    }
    return keyNames;
  }
  
  /**
   * Extract a {@link ModelMetadata} from an instance of a model object.
   * 
   * @param object
   * @param config
   *          must not be null.
   * @return
   */
  private ModelMetadata getModelMetadata(Object object, DynamoDBMapperConfig config) {
    Class<?> clazz = object.getClass();
    String tableName = getTableName(clazz, config);
    String[] keyNames = getKeyNames(clazz);
    String hashKeyName = keyNames[0];
    String rangeKeyName = keyNames.length == 2 ? keyNames[1] : null;
    Method hashKeyGetter = reflector.getHashKeyGetter(clazz);
    AttributeValue hashKeyValue = getHashKeyElement(safeInvoke(hashKeyGetter, object), hashKeyGetter);
    Method rangeKeyGetter = reflector.getRangeKeyGetter(object.getClass());
    AttributeValue rangeKeyValue = rangeKeyGetter == null ? null : getRangeKeyElement(
        safeInvoke(rangeKeyGetter, object), rangeKeyGetter);

    return new ModelMetadata(tableName, hashKeyName, rangeKeyName, hashKeyValue, rangeKeyValue);
  }

}
