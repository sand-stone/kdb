package kdb;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.asynchttpclient.*;
import java.util.concurrent.Future;

import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;
import kdb.proto.XMessage.DropOperation;

public class KdbIntegrationTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public KdbIntegrationTest(String testName)
  {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite()
  {
    return new TestSuite(KdbIntegrationTest.class);
  }

  public void test0() {
    Client client = new Client();
    String table = "test0";
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildCreateOp(table));
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildDropOp(table));
  }

  public void test1() {
    Client client = new Client();
    String table = "test1";
    int c = 3;
    while (c-->0) {
      client.sendMsg("http://localhost:8000/service", MessageBuilder.buildCreateOp(table));
      client.sendMsg("http://localhost:8000/service", MessageBuilder.buildDropOp(table));
    }
  }

  public void test2() {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "testKey".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "testvalue".getBytes());
    Client client = new Client();
    String table = "test2";
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildCreateOp(table));
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildInsertOp(table, keys, values));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "testKey".getBytes()));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    //client.sendMsg("http://localhost:8000/service", MessageBuilder.buildDropOp(table));
    assertTrue(true);
  }

  public void test3() {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "key2".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "val2".getBytes());
    Client client = new Client();
    String table = "test3";
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildCreateOp(table));
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildInsertOp(table, keys, values));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "key2".getBytes()));
    client.sendMsg("http://localhost:8001/service", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "key2".getBytes()));
    client.sendMsg("http://localhost:8002/service", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "key2".getBytes()));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    //client.sendMsg("http://localhost:8000/service", MessageBuilder.buildDropOp(table));
    assertTrue(true);
  }
}
