package com.dailycred.dynamonito.core;

import java.lang.annotation.*;

/**
 * Annotate a DynamoDB model class to cache it.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface DynamoDBCache {

  /**
   * Time to live in seconds for a cached item after written to cache. 0 means forever.
   * 
   * @return
   */
  int ttl() default 0;
}
