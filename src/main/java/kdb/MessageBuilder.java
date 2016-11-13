package kdb;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import kdb.proto.XMessage;
import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.Message.MessageType;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;
import kdb.proto.XMessage.CreateOperation;
import kdb.proto.XMessage.DropOperation;
import kdb.proto.XMessage.LogOperation;
import kdb.proto.XMessage.Response;

final class MessageBuilder {
  final static Message nullMsg = MessageBuilder.buildErrorResponse("null");
  final static Message emptyMsg = MessageBuilder.buildResponse("null");
  final static Message busyMsg = MessageBuilder.buildRetryResponse("server side busy");

  private MessageBuilder() {}

  public static Message buildErrorResponse(String error) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.Error)
      .setReason(error)
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildRetryResponse(String msg) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.Retry)
      .setReason(msg)
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildResponse(String msg) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.OK)
      .setReason(msg)
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildResponse(byte[] key, byte[] value) {
    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    keys.add(key);
    ArrayList<byte[]> values = new ArrayList<byte[]>();
    values.add(value);
    return buildResponse("", keys, values);
  }

  public static Message buildResponse(String token, List<byte[]> keys, List<byte[]> values) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.OK)
      .setReason("OK")
      .setToken(token)
      .addAllKeys(keys.stream().map(k -> ByteString.copyFrom(k)).collect(toList()))
      .addAllValues(values.stream().map(v -> ByteString.copyFrom(v)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildResponse(byte[] values) {
    ArrayList<byte[]> list = new ArrayList<byte[]>();
    list.add(values);
    return buildResponse(list);
  }

  public static Message buildResponse(List<byte[]> values) {
    Response op = Response
      .newBuilder()
      .setType(Response.Type.OK)
      .setReason("OK")
      .addAllValues(values.stream().map(v -> ByteString.copyFrom(v)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Response).setResponse(op).build();
  }

  public static Message buildCreateOp(String table) {
    CreateOperation op = CreateOperation
      .newBuilder()
      .setTable(table)
      .build();
    return Message.newBuilder().setType(MessageType.Create).setCreateOp(op).build();
  }

  public static Message buildDropOp(String table) {
    DropOperation op = DropOperation
      .newBuilder()
      .setTable(table)
      .build();
    return Message.newBuilder().setType(MessageType.Drop).setDropOp(op).build();
  }

  public static Message buildInsertOp(String table, List<byte[]> keys, List<byte[]> values) {
    InsertOperation op = InsertOperation
      .newBuilder()
      .setTable(table)
      .addAllKeys(keys.stream().map(k -> ByteString.copyFrom(k)).collect(toList()))
      .addAllValues(values.stream().map(v -> ByteString.copyFrom(v)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Insert).setInsertOp(op).build();
  }

  public static Message buildUpdateOp(String table, List<byte[]> keys) {
    UpdateOperation op = UpdateOperation
      .newBuilder()
      .setTable(table)
      .addAllKeys(keys.stream().map(k -> ByteString.copyFrom(k)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Update).setUpdateOp(op).build();
  }

  public static Message buildUpdateOp(String table, List<byte[]> keys, List<byte[]> values) {
    UpdateOperation op = UpdateOperation
      .newBuilder()
      .setTable(table)
      .addAllKeys(keys.stream().map(k -> ByteString.copyFrom(k)).collect(toList()))
      .addAllValues(values.stream().map(v -> ByteString.copyFrom(v)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Update).setUpdateOp(op).build();
  }

  public static Message buildGetOp(String table, GetOperation.Type opt, byte[] key) {
    return buildGetOp(table, opt, key, 1);
  }

  public static Message buildGetOp(String token, GetOperation.Type opt, int limit) {
    GetOperation op = GetOperation
      .newBuilder()
      .setOp(opt)
      .setToken(token)
      .setLimit(limit)
      .build();
    return Message.newBuilder().setType(MessageType.Get).setGetOp(op).build();
  }

  public static Message buildGetOp(String table, GetOperation.Type opt, byte[] key, int limit) {
    GetOperation op = GetOperation
      .newBuilder()
      .setTable(table)
      .setOp(opt)
      .setKey(ByteString.copyFrom(key))
      .setLimit(limit)
      .build();
    return Message.newBuilder().setType(MessageType.Get).setGetOp(op).build();
  }

  public static Message buildGetOp(String table, byte[] key, byte[] key2) {
    return buildGetOp(table, key, key2, 1);
  }

  public static Message buildGetOp(String table, byte[] key, byte[] key2, int limit) {
    GetOperation op = GetOperation
      .newBuilder()
      .setTable(table)
      .setOp(GetOperation.Type.Between)
      .setKey(ByteString.copyFrom(key))
      .setKey2(ByteString.copyFrom(key2))
      .setLimit(limit)
      .build();
    return Message.newBuilder().setType(MessageType.Get).setGetOp(op).build();
  }

  public static Message buildLogOp(Message msg, long epoch, long xid, int file, long offset) {
    LogOperation op = LogOperation
      .newBuilder()
      .setEpoch(epoch)
      .setXid(xid)
      .setLsnfile(file)
      .setLsnoffset(offset)
      .setUri(msg.getLogOp().getUri())
      .setTable(msg.getLogOp().getTable())
      .build();
    return Message.newBuilder().setType(MessageType.Log).setLogOp(op).build();
  }

  public static Message buildLogOp(String uri, String table, long epoch, long xid) {
    LogOperation op = LogOperation
      .newBuilder()
      .setEpoch(epoch)
      .setXid(xid)
      .setUri(uri)
      .setTable(table)
      .build();
    return Message.newBuilder().setType(MessageType.Log).setLogOp(op).build();
  }
}
