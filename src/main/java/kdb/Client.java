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
      //log.info("r: {}", r);
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

}
