package com.dailycred.dynamonito.util;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.transform.AttributeValueJsonUnmarshaller;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class Util {

  /**
   * Parses an AttributeValue into a Json object.
   * 
   * @param av
   * @return
   */
  public static ObjectNode getJsonObjFromAttributeValue(AttributeValue av) {
    if (av == null)
      return null;
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    if (av.getS() != null)
      obj.put("S", av.getS());
    if (av.getN() != null)
      obj.put("N", av.getN());
    if (av.getB() != null) {
      obj.put("B", byteBufferToString(av.getB()));
    }

    if (av.getSS() != null) {
      ArrayNode array = JsonNodeFactory.instance.arrayNode();
      for (int i = 0; i < av.getSS().size(); i++) {
        array.add(new TextNode(av.getSS().get(i)));
      }
      obj.put("SS", array);
    }
    if (av.getNS() != null) {
      ArrayNode array = JsonNodeFactory.instance.arrayNode();
      for (int i = 0; i < av.getNS().size(); i++) {
        array.add(new TextNode(av.getNS().get(i)));
      }
      obj.put("NS", array);
    }
    if (av.getBS() != null) {
      ArrayNode array = JsonNodeFactory.instance.arrayNode();
      for (int i = 0; i < av.getBS().size(); i++) {
        array.add(new TextNode(byteBufferToString(av.getBS().get(i))));
      }
      obj.put("BS", array);
    }
    return obj;
  }

  /**
   * Get the object representation of a {@link AttributeValue}
   * 
   * @param av
   * @return
   */
  public static Object getKeyValueFromAttributeValue(AttributeValue av) {
    Preconditions.checkNotNull(av);
    if (av.getS() != null)
      return av.getS();
    if (av.getN() != null)
      return av.getN();
    if (av.getB() != null) {
      return byteBufferToString(av.getB());
    }
    throw new IllegalArgumentException("AttributeValue does not contains a String, Number, nor Binary value. av is "
        + av.toString());
  }

  /**
   * Make a base64 string from a ByteBuffer
   * 
   * @param b
   * @return
   */
  public static String byteBufferToString(ByteBuffer b) {
    b.mark();
    byte[] bytes = new byte[b.capacity()];
    b.get(bytes, 0, bytes.length);
    b.reset();
    return (BinaryUtils.toBase64(bytes));
  }

  public static final JsonFactory fac = new JsonFactory();

  /**
   * itemJson is a Gson object that represents a DynamoDB item. parseItemJson parses the object into a map of attribute
   * name to value pairs.
   * 
   * @param itemJson
   * @return
   * @throws Exception
   */
  public static Map<String, AttributeValue> parseItemJson(JsonNode itemJson) throws Exception {
    Map<String, AttributeValue> item = Maps.newHashMap();
    for (Iterator<Entry<String, JsonNode>> iter = itemJson.getFields(); iter.hasNext();) {
      Entry<String, JsonNode> en = iter.next();
      item.put(en.getKey(), getAttributeValue(en.getValue().toString()));
    }
    return item;
  }

  public static String toRedisKey(String tableName, Key key) {
    AttributeValue hashAV = key.getHashKeyElement();
    String hashVal = getKeyValueFromAttributeValue(hashAV).toString();
    AttributeValue rangeAV = key.getRangeKeyElement();
    if (rangeAV == null) {
      return tableName + ":" + hashVal;
    } else {
      String rangeVal = getKeyValueFromAttributeValue(rangeAV).toString();
      return tableName + ":" + hashVal + ":" + rangeVal;
    }
  }

  /**
   * Parse a json snippet to get AttributeValue
   * 
   * @param value
   * @return
   * @throws Exception
   */
  public static AttributeValue getAttributeValue(String value) throws Exception {
    JsonParser avParser = fac.createJsonParser(value.toString());
    JsonUnmarshallerContext context = new JsonUnmarshallerContext(avParser);
    AttributeValue attributeValue = AttributeValueJsonUnmarshaller.getInstance().unmarshall(context);
    return attributeValue;
  }

  /**
   * Given some item updates, figure out what attributes need to be put and what attributes need to be removed.
   * 
   * @param existingAVs
   * @param attributeUpdates
   * @return _1 is puts, _2 is removes
   * @throws JSONException
   */
  public static Tuple2<Map<String, AttributeValue>, Set<String>> applyUpdatedAttributeValues(
      Map<String, AttributeValue> existingAVs,
      Map<String, AttributeValueUpdate> attributeUpdates)
      throws JSONException {
    Map<String, AttributeValue> puts = Maps.newHashMap();
    Set<String> deletes = Sets.newHashSet();
    for (Entry<String, AttributeValueUpdate> en : attributeUpdates.entrySet()) {
      AttributeValue updateAV = en.getValue() == null ? null : en.getValue().getValue();
      AttributeValue existingAV = existingAVs.get(en.getKey());
      if (en.getValue().getAction().equals("ADD")) {

        if (existingAV != null) {
          if (existingAV.getN() != null) {
            // Let's do some number calculation
            BigDecimal extAmt = new BigDecimal(existingAV.getN());
            BigDecimal addAmt = new BigDecimal(updateAV.getN());
            BigDecimal sumAmt = extAmt.add(addAmt);
            puts.put(en.getKey(), new AttributeValue().withN(sumAmt.toPlainString()));
          } else if (existingAV.getNS() != null) {
            if (updateAV.getN() != null) {
              /**
               * Add N to NS
               */
              String nToAdd = updateAV.getN();
              // Double check that nToAdd is indeed a number
              new BigDecimal(nToAdd);
              existingAV.getNS().add(nToAdd);
              puts.put(en.getKey(), existingAV);
            } else if (updateAV.getNS() != null) {
              /**
               * Add NS to NS. Need this Set to List conversion here because AttributeValue uses List to represent Set.
               */
              Set<String> existingNs = new HashSet<String>(existingAV.getNS());
              for (String n : updateAV.getNS()) {
                existingNs.add(n);
              }
              existingAV.setNS(existingNs);
              puts.put(en.getKey(), existingAV);
            } else {
              throw new RuntimeException("bad state");
            }
          } else if (existingAV.getBS() != null) {
            ByteBuffer bToAdd = updateAV.getB();
            existingAV.getBS().add(bToAdd);
            puts.put(en.getKey(), existingAV);
          } else if (existingAV.getSS() != null) {
            String sToAdd = updateAV.getS();
            existingAV.getSS().add(sToAdd);
            puts.put(en.getKey(), existingAV);
          } else {
            throw new IllegalStateException("incompatible type found during update ADD value.");
          }
        } else {
          // No existing val, just do a put
          puts.put(en.getKey(), updateAV);
        }
      } else if (en.getValue().getAction().equals("PUT")) {
        puts.put(en.getKey(), updateAV);
      } else if (en.getValue().getAction().equals("DELETE")) {
        /**
         * Are we trying to delete an entire attribute or elements from a "set attribute"? For the former, simply delete
         * the field. For the latter, first construct the set difference, then put the difference in the "put set".
         */
        if (updateAV == null || updateAV.getN() != null || updateAV.getS() != null || updateAV.getB() != null) {
          deletes.add(en.getKey());
        } else if (updateAV.getNS() != null) {
          Set<String> valSet = new HashSet<String>(existingAV.getNS());
          for (String n : updateAV.getNS()) {
            valSet.remove(n);
          }
          if (valSet.isEmpty())
            deletes.add(en.getKey());
          else
            puts.put(en.getKey(), new AttributeValue().withNS(valSet));
        } else if (updateAV.getSS() != null) {
          Set<String> valSet = new HashSet<String>(existingAV.getSS());
          for (String n : updateAV.getNS()) {
            valSet.remove(n);
          }
          if (valSet.isEmpty())
            deletes.add(en.getKey());
          else
            puts.put(en.getKey(), new AttributeValue().withSS(valSet));
        } else if (updateAV.getBS() != null) {
          Set<ByteBuffer> valSet = new HashSet<ByteBuffer>(existingAV.getBS());
          for (ByteBuffer n : updateAV.getBS()) {
            valSet.remove(n);
          }
          if (valSet.isEmpty())
            deletes.add(en.getKey());
          else
            puts.put(en.getKey(), new AttributeValue().withBS(valSet));
        }
      }

    }
    return Tuple2.build(puts, deletes);
  }

  public static String getStringFromAttributeValue(AttributeValue value) throws JSONException {
    StringWriter stringWriter = new StringWriter();
    JSONWriter jsonWriter = new JSONWriter(stringWriter);
    // Following is copied from UpdateItemRequestMarshaller
    // @formatter:off
    if (value != null) {

      jsonWriter.object();

      if (value.getS() != null) {
        jsonWriter.key("S").value(value.getS());
      }
      if (value.getN() != null) {
        jsonWriter.key("N").value(value.getN());
      }
      if (value.getB() != null) {
        jsonWriter.key("B").value(value.getB());
      }

      java.util.List<String> sSList = value.getSS();
      if (sSList != null && sSList.size() > 0) {

        jsonWriter.key("SS");
        jsonWriter.array();

        for (String sSListValue : sSList) {
          if (sSListValue != null) {
            jsonWriter.value(sSListValue);
          }
        }
        jsonWriter.endArray();
      }

      java.util.List<String> nSList = value.getNS();
      if (nSList != null && nSList.size() > 0) {

        jsonWriter.key("NS");
        jsonWriter.array();

        for (String nSListValue : nSList) {
          if (nSListValue != null) {
            jsonWriter.value(nSListValue);
          }
        }
        jsonWriter.endArray();
      }

      java.util.List<java.nio.ByteBuffer> bSList = value.getBS();
      if (bSList != null && bSList.size() > 0) {

        jsonWriter.key("BS");
        jsonWriter.array();

        for (java.nio.ByteBuffer bSListValue : bSList) {
          if (bSListValue != null) {
            jsonWriter.value(bSListValue);
          }
        }
        jsonWriter.endArray();
      }
      jsonWriter.endObject();
    }
    // @formatter:on
    return stringWriter.toString();
  }

}
