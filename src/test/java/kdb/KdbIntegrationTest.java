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

  /**
   * Rigourous Test :-)
   */
  public void testApp()
  {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "key2".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "val2".getBytes());
    Client client = new Client();
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildCreateOp("foo"));
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildInsertOp("foo", keys, values));
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    client.sendMsg("http://localhost:8000/service", MessageBuilder.buildGetOp("foo", GetOperation.Type.Equal, "key2".getBytes()));
    client.sendMsg("http://localhost:8001/service", MessageBuilder.buildGetOp("foo", GetOperation.Type.Equal, "key2".getBytes()));
    client.sendMsg("http://localhost:8002/service", MessageBuilder.buildGetOp("foo", GetOperation.Type.Equal, "key2".getBytes()));
    assertTrue(true);
  }
}
