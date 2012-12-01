package com.amazonaws.services.dynamodb.datamodeling;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * {@link DynamoDBReflector} contains methods for retrieving the model class's metadata. Unfortunatelly
 * the DynamoDBReflector's methods are not public. DynamonitoReflector wraps the DynamoDBReflector and delegates some of the methods
 * using reflection.
 * 
 */
public class DynamonitoReflector {

  private DynamoDBReflector reflector;

  public DynamonitoReflector(DynamoDBMapper mapper) {
    try {
      Field reflectorField = mapper.getClass().getDeclaredField("reflector");
      reflectorField.setAccessible(true);
      reflector = (DynamoDBReflector) reflectorField.get(mapper);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void invoke(DynamoDBMapper mapper) throws IllegalArgumentException, IllegalAccessException, SecurityException,
      NoSuchFieldException {
  }

  public <T> Method getHashKeyGetter(Class<T> clazz) {
    return reflector.getHashKeyGetter(clazz);
  }

  public <T> Method getRangeKeyGetter(Class<T> clazz) {
    return reflector.getRangeKeyGetter(clazz);
  }

  public ArgumentMarshaller getArgumentMarshaller(Method getter) {
    return reflector.getArgumentMarshaller(getter);
  }
  
  public  String getAttributeName(Method getter) {
    return reflector.getAttributeName(getter);
  }
}
