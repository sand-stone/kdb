package kdb;

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

public final class Client implements Closeable {
  private static Logger log = LogManager.getLogger(Client.class);
  final AsyncHttpClientConfig config;
  AsyncHttpClient client;

  public Client() {
    config = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(Integer.MAX_VALUE).build();
    client = new DefaultAsyncHttpClient(config);
  }

  public void sendMsg(String url, Message msg) {
    try {
      Response r;
      r=client.preparePost(url)
        .setBody(msg.toByteArray())
        .execute()
        .get();
      log.info("r: {}", r);
    } catch(InterruptedException e) {
      log.info(e);
    } catch(ExecutionException e) {
      log.info(e);
    }
  }

  public void close() {
    try {
      client.close();
    } catch(IOException e) {}
  }

  private static Message genInsertReq() {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "key2".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "val2".getBytes());
    return MessageBuilder.buildInsertOp(keys, values);
  }

  private static Message genGetReq() {
    return MessageBuilder.buildGetOp(GetOperation.Type.Equal, "key2".getBytes());
  }

  public static void main(String[] args) {
    Client client = new Client();
    client.sendMsg("http://localhost:8000/insert", genInsertReq());
    client.sendMsg("http://localhost:8000/get", genGetReq());
  }

}
