package kdb;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.asynchttpclient.*;
import java.util.concurrent.Future;

import kdb.proto.XMessage.Message;
import kdb.proto.XMessage;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;
import kdb.proto.XMessage.DropOperation;
import com.google.protobuf.InvalidProtocolBufferException;

public final class Client implements Closeable {
  private static Logger log = LogManager.getLogger(Client.class);
  AsyncHttpClient client;
  int timeout;
  private String uri;
  private String table;
  private String token;

  public enum QueryType {
    Equal,
    GreaterEqual,
    LessEqual,
    Between
  }

  public enum Status {
    OK,
    Error,
    Retry
  }

  public static class Result {
    XMessage.Response rsp;

    Result(XMessage.Response rsp) {
      this.rsp = rsp;
    }

    public Status status() {
      switch(rsp.getType()) {
      case Error:
        return Status.Error;
      case Retry:
        return Status.Retry;
      }
      return Status.OK;
    }

    public String token() {
      return rsp.getToken();
    }

    public int count() {
      return rsp.getKeysCount();
    }

    public List<byte[]> keys() {
      return rsp.getKeysList().stream().map(k -> k.toByteArray()).collect(toList());
    }

    public List<byte[]> values() {
      return rsp.getValuesList().stream().map(v -> v.toByteArray()).collect(toList());
    }

    public byte[] getKey(int index) {
      return rsp.getKeys(index).toByteArray();
    }

    public byte[] getValue(int index) {
      return rsp.getValues(index).toByteArray();
    }

    public String toString() {
      return rsp.toString();
    }
  }

  public Client(String uri, String table, int timeout) {
    try {
      final AsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(timeout).build();
      client = new DefaultAsyncHttpClient(config);
    } catch(Exception e) {
      throw new KdbException(e);
    }
    this.uri = uri;
    this.table = table;
    this.token = "";
  }

  public Client(String uri, String table) {
    this(uri, table, Integer.MAX_VALUE);
  }

  public Client(String uri) {
    this(uri, null);
  }

  public static Result createTable(String uri, String table) {
    Message msg;
    try(Client client = new Client(uri)) {
      msg = client.sendMsg(MessageBuilder.buildCreateOp(table));
    }
    return new Result(msg.getResponse());
  }

  public static Result dropTable(String uri, String table) {
    Message msg;
    try(Client client = new Client(uri)) {
      msg = client.sendMsg(MessageBuilder.buildDropOp(table));
    }
    return new Result(msg.getResponse());
  }

  public Result insert(List<byte[]> keys, List<byte[]> values) {
    Message msg = sendMsg(MessageBuilder.buildInsertOp(table, keys, values));
    return new Result(msg.getResponse());
  }

  public Result update(List<byte[]> keys, List<byte[]> values) {
    Message msg = sendMsg(MessageBuilder.buildUpdateOp(table, keys, values));
    return new Result(msg.getResponse());
  }

  public Result increment(List<byte[]> keys) {
    Message msg = sendMsg(MessageBuilder.buildUpdateOp(table, keys));
    return new Result(msg.getResponse());
  }

  public Result get(QueryType type, byte[] key, int limit) {
    GetOperation.Type op = GetOperation.Type.Done;
    switch(type) {
    case Equal:
      op = GetOperation.Type.Equal;
      break;
    case GreaterEqual:
      op = GetOperation.Type.GreaterEqual;
      break;
    case LessEqual:
      op = GetOperation.Type.LessEqual;
      break;
    default:
      throw new KdbException("unknown query type");
    }
    Message msg = sendMsg(MessageBuilder.buildGetOp(table, op, key, limit));
    token = msg.getResponse().getToken();
    return new Result(msg.getResponse());
  }

  public Result get(QueryType type, String token, int limit) {
    GetOperation.Type op = GetOperation.Type.Done;
    switch(type) {
    case Equal:
      op = GetOperation.Type.Equal;
      break;
    case GreaterEqual:
      op = GetOperation.Type.GreaterEqual;
      break;
    case LessEqual:
      op = GetOperation.Type.LessEqual;
      break;
    case Between:
      op = GetOperation.Type.Between;
      break;
    }
    Message msg = sendMsg(MessageBuilder.buildGetOp(token, op, limit));
    token = msg.getResponse().getToken();
    return new Result(msg.getResponse());
  }

  public Result get(byte[] key1, byte[] key2, int limit) {
    Message msg = sendMsg(MessageBuilder.buildGetOp(table, key1, key2, limit));
    token = msg.getResponse().getToken();
    return new Result(msg.getResponse());
  }

  private Message sendMsg(Message msg) {
    Message rsp = MessageBuilder.nullMsg;
    try {
      Response r = client
        .preparePost(uri)
        .setBody(msg.toByteArray())
        .execute()
        .get();
      byte[] data = r.getResponseBodyAsBytes();
      rsp = Message.parseFrom(data);
    } catch(InterruptedException e) {
      log.debug(e);
      //e.printStackTrace();
    } catch(ExecutionException e) {
      log.debug(e);
      throw new KdbException(e);
      //e.printStackTrace();
    } catch(InvalidProtocolBufferException e) {
      log.debug(e);
      throw new KdbException(e);
      //e.printStackTrace();
    }
    return rsp;
  }

  private void releaseToken() {
    if(table != null && token.length() > 0) {
      sendMsg(MessageBuilder.buildGetOp(token, GetOperation.Type.Done, 0));
    }
    table = null;
    token = "";
  }

  public void close() {
    try {
      releaseToken();
    } catch(Exception e) {}
  }

}
