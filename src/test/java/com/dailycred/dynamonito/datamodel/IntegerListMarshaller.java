package com.dailycred.dynamonito.datamodel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMarshaller;

/**
 * Marshall a list of integers into a CSV
 * 
 */
public class IntegerListMarshaller implements DynamoDBMarshaller<List<Integer>> {

  @Override
  public String marshall(List<Integer> getterReturnResult) {
    if (getterReturnResult == null || getterReturnResult.isEmpty())
      return null;
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < getterReturnResult.size(); i++) {
      sb.append(getterReturnResult.get(i));
      if (i < getterReturnResult.size() - 1)
        sb.append(',');
    }
    return sb.toString();
  }

  @Override
  public List<Integer> unmarshall(Class<List<Integer>> clazz, String obj) {
    if (obj == null || obj.isEmpty())
      return Collections.emptyList();
    String[] split = obj.split(",");
    List<Integer> ls = new ArrayList<Integer>(split.length);
    for (int i = 0; i < split.length; i++) {
      ls.add(i, Integer.valueOf(split[i]));
    }
    return ls;
  }
}
