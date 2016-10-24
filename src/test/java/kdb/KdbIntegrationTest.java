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
  private static Logger log = LogManager.getLogger(KdbIntegrationTest.class);

  public KdbIntegrationTest(String testName) {
    super(testName);
  }

  public static Test suite()  {
    return new TestSuite(KdbIntegrationTest.class);
  }

  public void test0() {
    Client client = new Client();
    String table = "test0";
    Message msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildCreateOp(table));
    try { Thread.currentThread().sleep(500); } catch(Exception e) {}
    msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildDropOp(table));
    //log.info("msg {}", msg);
    assertTrue(true);
  }

  public void test1() {
    Client client = new Client();
    String table = "test1";
    int c = 5;
    while (c-->0) {
      client.sendMsg("http://localhost:8000/", MessageBuilder.buildCreateOp(table));
      try { Thread.currentThread().sleep(500); } catch(Exception e) {}
      client.sendMsg("http://localhost:8000/", MessageBuilder.buildDropOp(table));
    }
  }

  public void test2() {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "testKey".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "testvalue".getBytes());
    Client client = new Client();
    String table = "test2";
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildCreateOp(table));
    try { Thread.currentThread().sleep(500); } catch(Exception e) {}
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildInsertOp(table, keys, values));
    try { Thread.currentThread().sleep(500); } catch(Exception e) {}
    Message msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "testKey".getBytes()));
    if(msg.getResponse().getValuesCount() == 1 && new String(msg.getResponse().getValues(0).toByteArray()).equals("testvalue")) {
      assertTrue(true);
    } else
      assertTrue(false);
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildDropOp(table));
  }

  public void test3() {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "key2".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "val2".getBytes());
    Client client = new Client();
    String table = "test3";
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildCreateOp(table));
    try { Thread.currentThread().sleep(500); } catch(Exception e) {}
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildInsertOp(table, keys, values));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    Message msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "key2".getBytes()));
    msg = client.sendMsg("http://localhost:8001/", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "key2".getBytes()));
    msg = client.sendMsg("http://localhost:8002/", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "key2".getBytes()));
    if(msg.getResponse().getValuesCount() == 1 && new String(msg.getResponse().getValues(0).toByteArray()).equals("val2")) {
      assertTrue(true);
    } else
      assertTrue(false);
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildDropOp(table));
  }

  public void test4() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("test4key"+i).getBytes());
      values.add(("test4value"+i).getBytes());
    }
    Client client = new Client();
    String table = "test4";
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildCreateOp(table));
    try { Thread.currentThread().sleep(500); } catch(Exception e) {}
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildInsertOp(table, keys, values));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    Message get = MessageBuilder.buildGetOp(table, GetOperation.Type.GreaterEqual, "test4key2".getBytes(), 5);
    Message msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildGetOp(table, GetOperation.Type.GreaterEqual, "test4key2".getBytes(), 5));
    if(msg.getResponse().getValuesCount() == 5) {
      msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildGetOp(msg.getResponse().getToken(), GetOperation.Type.GreaterEqual, 5));
      if(msg.getResponse().getValuesCount() == 3) {
        assertTrue(true);
      } else
        assertTrue(false);
    } else
      assertTrue(false);
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildDropOp(table));
  }

  public void test5() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("key"+i).getBytes());
      values.add(("value-1-"+i).getBytes());
    }
    Client client = new Client();
    String table = "test5";
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildCreateOp(table));
    try { Thread.currentThread().sleep(500); } catch(Exception e) {}
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildInsertOp(table, keys, values));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    keys.clear();
    values.clear();
    for (int i = 0; i < count; i++) {
      keys.add(("key"+i).getBytes());
      values.add(("value-2-"+i).getBytes());
    }
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildUpdateOp(table, keys, values));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    Message msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildGetOp(table, GetOperation.Type.GreaterEqual, "key2".getBytes(), 5));
    //log.info("msg {}", msg);
    if(msg.getResponse().getValuesCount() == 5) {
       assertTrue(true);
    } else {
      assertTrue(false);
    }
    msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildGetOp(msg.getResponse().getToken(), GetOperation.Type.Done, 0));
    try { Thread.currentThread().sleep(100); } catch(Exception e) {}
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildDropOp(table));
    //log.info("drop table");
  }

}
