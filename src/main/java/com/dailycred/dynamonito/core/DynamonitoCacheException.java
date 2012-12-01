package com.dailycred.dynamonito.core;

public class DynamonitoCacheException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public DynamonitoCacheException(String message, Throwable cause) {
    super(message, cause);
  }

  public DynamonitoCacheException(Throwable cause) {
    super(cause);
  }

  public DynamonitoCacheException(String message) {
    super(message);
  }
}
