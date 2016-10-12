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

public final class Client implements Closeable {
  private static Logger log = LogManager.getLogger(Client.class);
  final AsyncHttpClientConfig config;
  AsyncHttpClient client;

  public Client() {
    config = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(Integer.MAX_VALUE).build();
    client = new DefaultAsyncHttpClient(config);
  }

  public void sendMsg(String url, Serializable evt) {
    try {
      ByteBuffer data = Serializer.serialize(evt);
      Response r;
      r=client.preparePost(url)
        .setBody(data.array())
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

  private static Table buildTestTable() {
    return Table.TableBuilder.Table("acme", t -> {
        t.column( c -> {
            c.name("col1");
            c.type(Table.ColumnType.Int32);
            c.key(true);
          });
        t.column( c -> {
            c.name("col2");
            c.type(Table.ColumnType.Varchar);
          });
      });
  }

  private static Message.UpsertTable genTestData() {
    List<String> names = Arrays.asList("col1", "col2");
    List<Object> values = new ArrayList<Object>();
    int[] col1 = { 1, 2, 3};
    String[] col2 = {"aaa", "bbb", "ccc"};
    values.add(col1);
    values.add(col2);
    return new Message.UpsertTable("acme", names, values);
  }

  public static void main(String[] args) {
    Client client = new Client();
    client.sendMsg("http://localhost:8000/createtable", new Message.CreateTable(buildTestTable(), 10));
    client.sendMsg("http://localhost:8000/upsertable"+"?table=acme&partition=0", genTestData());
    client.sendMsg("http://localhost:8000/upsertable"+"?table=acme&partition=1", genTestData());
  }

}
