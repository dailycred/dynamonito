package com.dailycred.dynamonito.core;

/**
 * Interface for a pluggable Dynamonito cache.
 */
public interface CacheAdaptor {

  /**
   * Put an object in the cache.
   * 
   * @param table
   *          the table's name
   * @param hashKey
   *          the object's hash key
   * @param rangeKey
   *          Use null if the table does not have a range key
   * @param value
   *          the serialized object
   * @param ttl
   *          the object's time-to-live value in seconds or 0 if the entry is persistent
   */
  public void put(String table, Object hashKey, Object rangeKey, String value, int ttl);

  /**
   * Get an object from the cache.
   * 
   * @param table
   *          the table's name
   * @param hashKey
   *          the object's hash key
   * @param rangeKey
   *          Use null if the table does not have a range key
   * @return the serialized object, null if it does not exist
   */
  public String get(String table, Object hashKey, Object rangeKey);

  /**
   * Remove an object from the cache.
   * 
   * @param table
   *          the table's name
   * @param hashKey
   *          the object's hash key
   * @param rangeKey
   *          Use null if the table does not have a range key
   */
  public void remove(String table, Object hashKey, Object rangeKey);

  /**
   * Remove all objects that belong to a table from the cache.
   * 
   * @param table
   *          the table's name
   */
  public void remove(String table);

  /**
   * Remove all objects from the cache.
   */
  public void removeAll();

  /**
   * Count the number of items in the cache for the table.
   * 
   * @param table
   * @return -1 if the cache is currently disabled
   */
  public int count(String table);

  /**
   * Checks whether the cache is enabled.
   * 
   * @return
   */
  public boolean isEnabled();

  /**
   * Sets whether the cache is enabled.
   * 
   * @param enableCache
   */
  public void setEnabled(boolean enableCache);

}