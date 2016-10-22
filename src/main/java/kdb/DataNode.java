package kdb;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;

public final class DataNode {
  private static Logger log = LogManager.getLogger(DataNode.class);
  private Store store;
  private boolean standalone;
  private Ring ring;

  public DataNode() {}

  public DataNode(Ring ring, Store store, boolean standalone) {
    this.ring = ring;
    this.store = store;
    this.standalone = standalone;
  }

  public Message process(Message msg) {
    Message r = MessageBuilder.nullMsg;
    try {
      String table;
      log.info("msg {}", msg);
      switch(msg.getType()) {
      case Create:
        if(standalone) {
          table = msg.getCreateOp().getTable();
          store.create(table);
        } else {
          ring.zab.send(ByteBuffer.wrap(msg.toByteArray()), null);
        }
        r = MessageBuilder.buildResponse("Create");
        break;
      case Drop:
        table = msg.getDropOp().getTable();
        store.drop(table);
        r = MessageBuilder.buildResponse("Drop");
        break;
      case Get:
        byte[] ret = null;
        table = msg.getGetOp().getTable();
        try(Store.Context ctx = store.getContext(table)) {
          r = store.get(ctx, msg);
        }
        break;
      case Insert:
        if(standalone) {
          table = msg.getInsertOp().getTable();
          try(Store.Context ctx = store.getContext(table)) {
            store.insert(ctx, msg);
          }
        } else {
          ring.zab.send(ByteBuffer.wrap(msg.toByteArray()), null);
        }
        r = MessageBuilder.buildResponse("Insert");
        break;
      case Update:
        if(standalone) {
          table = msg.getUpdateOp().getTable();
          try(Store.Context ctx = store.getContext(table)) {
            store.update(ctx, msg);
          }
        } else {
          ring.zab.send(ByteBuffer.wrap(msg.toByteArray()), null);
        }
        r = MessageBuilder.buildResponse("Update");
        break;
      }
    } catch(Exception e) {
      log.info(e);
    }
    return r;
  }

}
