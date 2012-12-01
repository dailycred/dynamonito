package com.dailycred.dynamonito.datamodel;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.dailycred.dynamonito.core.DynamoDBCache;

@DynamoDBCache()
@DynamoDBTable(tableName = "DataType")
public class DataType {
  /**
   * Keys
   */
  private String hashKey;
  private String rangeKey;

  /**
   * Supported primitive and primitive wrapper data types.<br>
   * refer to http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/JavaSDKHighLevel.html
   */
  private String string;

  private Boolean booleanWrapper;
  
  private boolean booleanPrimitive;

  private Byte byteWrapper;
  
  private byte bytePrimitive;

  private Date date;

  private Calendar calendar;

  private Long longWrapper;
  
  private long longPrimitive;

  private Integer integerWrapper;
  
  private int intPrimitive;

  private Double doubleWrapper;
  
  private double doublePrimitive;

  private Float floatWrapper;
  
  private float floatPrimitive;

  private BigDecimal bigDecimal;

  private BigInteger bigInteger;

  private Set<String> stringSet;
  
  private Set<Integer> integerSet;

  /**
   * Binary data.
   */
  private ByteBuffer binary;
  
  /**
   * Custom marshaller
   */
  private List<Integer> integerList;
  
  /**
   * Test ignored property. It is not a part of hashcode and equals calculations.
   */
  private String ignored;

  public DataType() {
  }

  public DataType(String hashKey,
      String rangeKey,
      String string,
      Boolean booleanWrapper,
      boolean booleanPrimitive,
      Byte byteWrapper,
      byte bytePrimitive,
      Date date,
      Calendar calendar,
      Long longWrapper,
      long longPrimitive,
      Integer integerWrapper,
      int intPrimitive,
      Double doubleWrapper,
      double doublePrimitive,
      Float floatWrapper,
      float floatPrimitive,
      BigDecimal bigDecimal,
      BigInteger bigInteger,
      Set<String> stringSet,
      Set<Integer> integerSet,
      ByteBuffer binary,
      List<Integer> integerList) {
    this.hashKey = hashKey;
    this.rangeKey = rangeKey;
    this.string = string;
    this.booleanWrapper = booleanWrapper;
    this.booleanPrimitive = booleanPrimitive;
    this.byteWrapper = byteWrapper;
    this.bytePrimitive = bytePrimitive;
    this.date = date;
    this.calendar = calendar;
    this.longWrapper = longWrapper;
    this.longPrimitive = longPrimitive;
    this.integerWrapper = integerWrapper;
    this.intPrimitive = intPrimitive;
    this.doubleWrapper = doubleWrapper;
    this.doublePrimitive = doublePrimitive;
    this.floatWrapper = floatWrapper;
    this.floatPrimitive = floatPrimitive;
    this.bigDecimal = bigDecimal;
    this.bigInteger = bigInteger;
    this.stringSet = stringSet;
    this.integerSet = integerSet;
    this.setBinary(binary);
    this.integerList = integerList;
  }

  @DynamoDBHashKey
  public String getHashKey() {
    return hashKey;
  }

  public void setHashKey(String hashKey) {
    this.hashKey = hashKey;
  }

  @DynamoDBRangeKey
  public String getRangeKey() {
    return rangeKey;
  }

  public void setRangeKey(String rangeKey) {
    this.rangeKey = rangeKey;
  }

  public String getString() {
    return string;
  }

  public void setString(String string) {
    this.string = string;
  }

  public Boolean getBooleanWrapper() {
    return booleanWrapper;
  }

  public void setBooleanWrapper(Boolean booleanWrapper) {
    this.booleanWrapper = booleanWrapper;
  }

  public boolean isBooleanPrimitive() {
    return booleanPrimitive;
  }

  public void setBooleanPrimitive(boolean booleanPrimitive) {
    this.booleanPrimitive = booleanPrimitive;
  }

  public Byte getByteWrapper() {
    return byteWrapper;
  }

  public void setByteWrapper(Byte byteWrapper) {
    this.byteWrapper = byteWrapper;
  }

  public byte getBytePrimitive() {
    return bytePrimitive;
  }

  public void setBytePrimitive(byte bytePrimitive) {
    this.bytePrimitive = bytePrimitive;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public Calendar getCalendar() {
    return calendar;
  }

  public void setCalendar(Calendar calendar) {
    this.calendar = calendar;
  }

  public Long getLongWrapper() {
    return longWrapper;
  }

  public void setLongWrapper(Long longWrapper) {
    this.longWrapper = longWrapper;
  }

  public long getLongPrimitive() {
    return longPrimitive;
  }

  public void setLongPrimitive(long longPrimitive) {
    this.longPrimitive = longPrimitive;
  }

  public Integer getIntegerWrapper() {
    return integerWrapper;
  }

  public void setIntegerWrapper(Integer integerWrapper) {
    this.integerWrapper = integerWrapper;
  }

  public int getIntPrimitive() {
    return intPrimitive;
  }

  public void setIntPrimitive(int intPrimitive) {
    this.intPrimitive = intPrimitive;
  }

  public Double getDoubleWrapper() {
    return doubleWrapper;
  }

  public void setDoubleWrapper(Double doubleWrapper) {
    this.doubleWrapper = doubleWrapper;
  }

  public double getDoublePrimitive() {
    return doublePrimitive;
  }

  public void setDoublePrimitive(double doublePrimitive) {
    this.doublePrimitive = doublePrimitive;
  }

  public Float getFloatWrapper() {
    return floatWrapper;
  }

  public void setFloatWrapper(Float floatWrapper) {
    this.floatWrapper = floatWrapper;
  }

  public float getFloatPrimitive() {
    return floatPrimitive;
  }

  public void setFloatPrimitive(float floatPrimitive) {
    this.floatPrimitive = floatPrimitive;
  }

  public BigDecimal getBigDecimal() {
    return bigDecimal;
  }

  public void setBigDecimal(BigDecimal bigDecimal) {
    this.bigDecimal = bigDecimal;
  }

  public BigInteger getBigInteger() {
    return bigInteger;
  }

  public void setBigInteger(BigInteger bigInteger) {
    this.bigInteger = bigInteger;
  }

  public Set<String> getStringSet() {
    return stringSet;
  }

  public void setStringSet(Set<String> stringSet) {
    this.stringSet = stringSet;
  }

  public Set<Integer> getIntegerSet() {
    return integerSet;
  }

  public void setIntegerSet(Set<Integer> integerSet) {
    this.integerSet = integerSet;
  }

  public ByteBuffer getBinary() {
    return binary;
  }

  public void setBinary(ByteBuffer binary) {
    this.binary = binary;
  }

  @DynamoDBMarshalling(marshallerClass = IntegerListMarshaller.class)
  public List<Integer> getIntegerList() {
    return integerList;
  }

  public void setIntegerList(List<Integer> integerList) {
    this.integerList = integerList;
  }

  @DynamoDBIgnore
  public String getIgnored() {
    return ignored;
  }

  public void setIgnored(String ignored) {
    this.ignored = ignored;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(hashKey,
        rangeKey,
        string,
        booleanWrapper,
        booleanPrimitive,
        byteWrapper,
        bytePrimitive,
        date,
        calendar,
        longWrapper,
        longPrimitive,
        integerWrapper,
        intPrimitive,
        doubleWrapper,
        doublePrimitive,
        floatWrapper,
        floatPrimitive,
        bigDecimal,
        bigInteger,
        stringSet,
        integerSet,
        binary,
        integerList,
        ignored);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DataType that = (DataType) obj;
    return Objects.equal(hashKey, that.hashKey) &&
        Objects.equal(rangeKey, that.rangeKey) &&
        Objects.equal(string, that.string) &&
        Objects.equal(booleanWrapper, that.booleanWrapper) &&
        Objects.equal(booleanPrimitive, that.booleanPrimitive) &&
        Objects.equal(byteWrapper, that.byteWrapper) &&
        Objects.equal(bytePrimitive, that.bytePrimitive) &&
        Objects.equal(date, that.date) &&
        Objects.equal(calendar, that.calendar) &&
        Objects.equal(longWrapper, that.longWrapper) &&
        Objects.equal(longPrimitive, that.longPrimitive) &&
        Objects.equal(integerWrapper, that.integerWrapper) &&
        Objects.equal(intPrimitive, that.intPrimitive) &&
        Objects.equal(doubleWrapper, that.doubleWrapper) &&
        Objects.equal(doublePrimitive, that.doublePrimitive) &&
        Objects.equal(floatWrapper, that.floatWrapper) &&
        Objects.equal(floatPrimitive, that.floatPrimitive) &&
        Objects.equal(bigDecimal, that.bigDecimal) &&
        Objects.equal(bigInteger, that.bigInteger) &&
        Objects.equal(stringSet, that.stringSet) &&
        Objects.equal(integerSet, that.integerSet) &&
        Objects.equal(binary, that.binary) &&
        Objects.equal(integerList, that.integerList);
  }

  /**
   * Convinience function to return a {@link DataType} with a random UUID as the hash key and current time in ms as the
   * range key.
   * 
   * @return
   */
  public static DataType buildModelWithRandomKey() {
    String hashKey = UUID.randomUUID().toString();
    String rangeKey = "" + System.currentTimeMillis();
    byte bytePrimitive = 0;
    Set<String> stringSet = Sets.newHashSet("a", "b");
    Set<Integer> integerSet = Sets.newHashSet(0, 1);
    ByteBuffer binary = ByteBuffer.wrap("binary".getBytes());
    List<Integer> integerList = Lists.newArrayList(0, 1);
    return new DataType(hashKey,
        rangeKey,
        "string",
        Boolean.FALSE,
        false,
        new Byte(bytePrimitive),
        bytePrimitive,
        new Date(),
        Calendar.getInstance(),
        new Long(0),
        0L,
        new Integer(0),
        0,
        new Double(0.0),
        0.0,
        new Float(0.0),
        0.0F,
        BigDecimal.ZERO,
        BigInteger.ZERO,
        stringSet,
        integerSet,
        binary,
        integerList);
  }
}
