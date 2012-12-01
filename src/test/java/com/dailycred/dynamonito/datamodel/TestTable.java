package com.dailycred.dynamonito.datamodel;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Set;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "test")
public class TestTable {

  public TestTable(String id, String name, Integer age) {
    this.id = id;
    this.name = name;
    this.age = age;
  }

  private String id;
  private String name;
  private Integer age;
  private Set<String> colors;
  private Date date;
  private ByteBuffer bin;

  @DynamoDBHashKey(attributeName = "id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getAge() {
    return age;
  }

  public void setAge(Integer age) {
    this.age = age;
  }

  public Set<String> getColors() {
    return colors;
  }

  public void setColors(Set<String> colors) {
    this.colors = colors;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public ByteBuffer getBin() {
    return bin;
  }

  public void setBin(ByteBuffer bin) {
    this.bin = bin;
  }
}
