package kdb;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import java.util.concurrent.Future;
import static java.util.stream. Collectors.*;
import java.util.stream.Collectors;

public class KdbIntegrationTest extends TestCase {
  private static Logger log = LogManager.getLogger(KdbIntegrationTest.class);

  public KdbIntegrationTest(String testName) {
    super(testName);
  }

  public static Test suite()  {
    return new TestSuite(KdbIntegrationTest.class);
  }

  public void test0() {
    String table = "test0";
    Client.Result rsp = Client.createTable("http://localhost:8000/", table);
    rsp = Client.dropTable("http://localhost:8000/", table);
    assertTrue(true);
  }

  public void test1() {
    Client client = new Client("http://localhost:8000/");
    String table = "test1";
    int c = 5;
    Client.Result rsp;
    while (c-->0) {
      rsp = Client.createTable("http://localhost:8000/", table);
      rsp = Client.dropTable("http://localhost:8000/", table);
    }
    assertTrue(true);
  }

  public void test2() {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "testKey".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "testvalue".getBytes());
    String table = "test2";
    Client.createTable("http://localhost:8000/", table);
    try (Client client = new Client("http://localhost:8000/", table)) {
      Client.Result rsp = client.insert(keys, values);
      //log.info("rsp {}", rsp);
      rsp = client.get(Client.QueryType.Equal, "testKey".getBytes(), 1);
      //log.info("rsp {}", rsp);
      if(rsp.count() == 1 && new String(rsp.getValue(0)).equals("testvalue")) {
        assertTrue(true);
      } else
        assertTrue(false);
    }
    Client.dropTable("http://localhost:8000/", table);
  }

  public void test3() {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "key2".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "val2".getBytes());
    String table = "test3";
    Client.createTable("http://localhost:8000/", table);
    try(Client client = new Client("http://localhost:8000/", table)) {
      client.insert(keys, values);
      client.get(Client.QueryType.Equal, "key2".getBytes(), 1);
      client.get(Client.QueryType.Equal, "key2".getBytes(), 1);
      Client.Result rsp = client.get(Client.QueryType.Equal, "key2".getBytes(), 1);
      if(rsp.count() == 1 && new String(rsp.getValue(0)).equals("val2")) {
        assertTrue(true);
      } else
        assertTrue(false);
    }
    Client.dropTable("http://localhost:8000/", table);
  }

  public void test4() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("test4key"+i).getBytes());
      values.add(("test4value"+i).getBytes());
    }
    String table = "test4";
    Client.createTable("http://localhost:8000/", table);
    try (Client client = new Client("http://localhost:8000/", table)) {
      client.insert(keys, values);
      Client.Result rsp = client.get(Client.QueryType.GreaterEqual, "test4key2".getBytes(), 5);
      if(rsp.count() == 5) {
        rsp = client.get(Client.QueryType.GreaterEqual, rsp.token(), 5);
        if(rsp.count() == 3) {
          assertTrue(true);
        } else
          assertTrue(false);
      } else
        assertTrue(false);
    }
    Client.dropTable("http://localhost:8000/", table);
  }

  public void test5() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("key"+i).getBytes());
      values.add(("value-1-"+i).getBytes());
    }
    String table = "test5";
    Client.createTable("http://localhost:8000/", table);
    try(Client client = new Client("http://localhost:8000/", table)) {
      client.insert(keys, values);
      keys.clear();
      values.clear();
      for (int i = 0; i < count; i++) {
        keys.add(("key"+i).getBytes());
        values.add(("value-2-"+i).getBytes());
      }
      client.update(keys, values);
      Client.Result rsp = client.get(Client.QueryType.GreaterEqual, "key2".getBytes(), 5);
      //log.info("msg {}", rsp);
      if(rsp.count() == 5) {
        assertTrue(true);
      } else {
        assertTrue(false);
      }
    }
    Client.dropTable("http://localhost:8000/", table);
  }

  public void test6() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("key"+i).getBytes());
      values.add(("value-1-"+i).getBytes());
    }
    String table = "test6";
    Client.createTable("http://localhost:8000/", table);
    try (Client client = new Client("http://localhost:8000/", table)) {
      client.insert(keys, values);
      Client.Result rsp = client.get("key3".getBytes(), "key6".getBytes(),5);
      //log.info("msg {}", rsp);
      if(rsp.count() == 4) {
        assertTrue(true);
      } else {
        assertTrue(false);
      }
    }
    Client.dropTable("http://localhost:8000/", table);
    //log.info("drop table");
  }

  public void test7() {
    int c = 2;

    while(c-->0) {
      String table = "test7";
      Client.createTable("http://localhost:8000/", table);
      try (Client client = new Client("http://localhost:8000/", table)) {
        int count = 10;
        List<byte[]> keys = new ArrayList<byte[]>();
        List<byte[]> values = new ArrayList<byte[]>();

        UUID guid1 = UUID.randomUUID();
        UUID guid2 = UUID.randomUUID();

        for (int i = 0; i < count; i++) {
          ByteBuffer key = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
          key.put((byte)0);
          key.putLong(guid1.getMostSignificantBits()).putLong(guid1.getLeastSignificantBits());
          key.put((byte)i);
          keys.add(key.array());
          values.add(("value"+i).getBytes());
        }
        client.update(keys, values);

        keys.clear();
        values.clear();

        for (int i = 0; i < count; i++) {
          ByteBuffer key = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
          key.put((byte)1);
          key.putLong(guid2.getMostSignificantBits()).putLong(guid2.getLeastSignificantBits());
          key.put((byte)i);
          keys.add(key.array());
          values.add(("value"+i).getBytes());
        }
        client.update(keys, values);

        keys.clear();
        values.clear();

        //ByteBuffer key = ByteBuffer.allocate().order(ByteOrder.BIG_ENDIAN);
        Client.Result rsp = client.get(new byte[]{0}, new byte[]{1}, 100);
        assertTrue(rsp.count() == 10);

        ByteBuffer key1 = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
        key1.put((byte)1);
        key1.putLong(guid2.getMostSignificantBits()).putLong(guid2.getLeastSignificantBits());
        key1.put((byte)0);
        ByteBuffer key2 = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
        key2.put((byte)1);
        key2.putLong(guid2.getMostSignificantBits()).putLong(guid2.getLeastSignificantBits());
        key2.put((byte)10);
        rsp = client.get(key1.array(), key2.array(), 100);
        //log.info("msg {} ==> {} ", rsp, rsp.count());
        assertTrue(rsp.count() == 10);
      }
      Client.dropTable("http://localhost:8000/", table);
    }
  }

  public void test8() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("key"+i).getBytes());
    }

    String table = "test8";
    Client.createTable("http://localhost:8000/", table);
    try (Client client = new Client("http://localhost:8000/", table)) {
      client.increment(keys);
      client.increment(keys);
      Client.Result rsp = client.get("key0".getBytes(), "key999".getBytes(), 50);
      //log.info("test 8 rsp {}", rsp);
      if(rsp.count() > 0) {
        rsp.values().stream().forEach(v -> assertTrue(ByteBuffer.wrap(v).order(ByteOrder.BIG_ENDIAN).getInt() == 2));
      } else {
        assertTrue(false);
      }
    }
    Client.dropTable("http://localhost:8000/", table);
  }

  public void test9() {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();

    String table = "test9";

    Client.createTable("http://localhost:8000/", table);
    try (Client client = new Client("http://localhost:8000/", table)) {
      for (int i = 0; i < count; i++) {
        keys.add(("key"+i).getBytes());
      }
      client.increment(keys);
      keys.clear();
      for (int i = 0; i < count/2; i++) {
        keys.add(("key"+i).getBytes());
      }
      client.increment(keys);
      Client.Result rsp = client.get("key0".getBytes(), "key999".getBytes(), 50);
      //log.info("test9 rsp:{}", rsp);
      if(rsp.count() > 0) {
        assertTrue(rsp.values().stream().filter(v -> ByteBuffer.wrap(v).order(ByteOrder.BIG_ENDIAN).getInt() == 2).count()
                   ==
                   rsp.values().stream().filter(v -> ByteBuffer.wrap(v).order(ByteOrder.BIG_ENDIAN).getInt() == 2).count());
      } else {
        assertTrue(false);
      }
    }
    Client.dropTable("http://localhost:8000/", table);
  }

}
