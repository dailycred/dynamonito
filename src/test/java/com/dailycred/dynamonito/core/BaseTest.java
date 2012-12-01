package com.dailycred.dynamonito.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;

public class BaseTest {
  private static AmazonDynamoDBClient client;
  private static Object clientLock = new Object();

  public static AmazonDynamoDB getClient() {
    final Properties prop;
    synchronized (clientLock) {
      if (client == null) {
        try {
          InputStream is = BaseTest.class.getClassLoader().getResourceAsStream("aws_credential.properties");
          prop = new Properties();
          prop.load(is);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        client = new AmazonDynamoDBClient(new BasicAWSCredentials(
            prop.getProperty("awsAccessKey"), prop.getProperty("awsSecretKey")));
        client.setEndpoint(prop.getProperty("awsEndpoint"));
      }
    }
    return client;
  }

  public static DynamoDBMapper getDynamoDBMapper() {
    return new DynamoDBMapper(getClient());
  }
}
