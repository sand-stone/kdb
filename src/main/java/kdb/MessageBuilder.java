package kdb;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import com.google.protobuf.ByteString;
import java.util.List;
import kdb.proto.XMessage;
import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.Message.MessageType;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;

final class MessageBuilder {

  private MessageBuilder() {}

  public static Message buildInsertOp(List<byte[]> keys, List<byte[]> values) {
    InsertOperation op = InsertOperation
      .newBuilder()
      .addAllKeys(keys.stream().map(k -> ByteString.copyFrom(k)).collect(toList()))
      .addAllValues(values.stream().map(v -> ByteString.copyFrom(v)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Insert).setInsertOp(op).build();
  }

  public static Message buildUpdateOp(List<byte[]> keys, List<byte[]> values) {
    UpdateOperation op = UpdateOperation
      .newBuilder()
      .addAllKeys(keys.stream().map(k -> ByteString.copyFrom(k)).collect(toList()))
      .addAllValues(values.stream().map(v -> ByteString.copyFrom(v)).collect(toList()))
      .build();
    return Message.newBuilder().setType(MessageType.Update).setUpdateOp(op).build();
  }

  public static Message buildGetOp(GetOperation.Type opt, byte[] key) {
    GetOperation op = GetOperation
      .newBuilder()
      .setOp(opt)
      .setKey(ByteString.copyFrom(key))
      .build();
    return Message.newBuilder().setType(MessageType.Get).setGetOp(op).build();
  }

}
