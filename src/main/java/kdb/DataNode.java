package kdb;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;
import kdb.rsm.ZabException;

public final class DataNode {
  private static Logger log = LogManager.getLogger(DataNode.class);
  private Store store;
  private boolean standalone;
  private Ring ring;
  private ConcurrentHashMap<String, Store.Context> ctxs;

  public DataNode() {
    this.ctxs = new ConcurrentHashMap<String, Store.Context>();
  }

  public DataNode(Ring ring, Store store, boolean standalone) {
    this.ring = ring;
    this.store = store;
    this.standalone = standalone;
    this.ctxs = new ConcurrentHashMap<String, Store.Context>();
  }

  public Message process(Message msg, Object context) throws ZabException.TooManyPendingRequests, ZabException.InvalidPhase {
    Message r = MessageBuilder.nullMsg;
    try {
      String table;
      Store.Context ctx;
      //log.info("msg {} context {}", msg, context);
      switch(msg.getType()) {
      case Create:
        if(standalone) {
          table = msg.getCreateOp().getTable();
          store.create(table);
        } else {
          ring.zab.send(ByteBuffer.wrap(msg.toByteArray()), context);
        }
        r = MessageBuilder.buildResponse("Create");
        break;
      case Drop:
        //log.info("msg {} context {}", msg, context);
        if(standalone) {
          table = msg.getDropOp().getTable();
          r = store.drop(table);
        } else {
          ring.zab.send(ByteBuffer.wrap(msg.toByteArray()), context);
        }
        break;
      case Get:
        String token = msg.getGetOp().getToken();
        //log.info("token <{}> ", token);
        if(token.equals("")) {
          table = msg.getGetOp().getTable();
          if(table != null && table.length() > 0)
            ctx = store.getContext(table);
          else {
            JettyTransport.reply(context, r);
            break;
          }
        } else {
          ctx = ctxs.get(token);
          if(ctx == null) {
            JettyTransport.reply(context, MessageBuilder.emptyMsg);
            break;
          }
        }
        try {
          r = store.get(ctx, msg);
        } finally {
          if(ctx.done) {
            ctxs.remove(ctx.token());
            ctx.close();
          } else {
            ctxs.putIfAbsent(ctx.token(), ctx);
          }
          JettyTransport.reply(context, r);
        }
        break;
      case Insert:
        if(standalone) {
          table = msg.getInsertOp().getTable();
          try(Store.Context c = store.getContext(table)) {
            store.insert(c, msg);
          }
        } else {
          ring.zab.send(ByteBuffer.wrap(msg.toByteArray()), context);
        }
        r = MessageBuilder.buildResponse("Insert");
        break;
      case Update:
        if(standalone) {
          table = msg.getUpdateOp().getTable();
          try(Store.Context c = store.getContext(table)) {
            store.update(c, msg);
          }
        } else {
          ring.zab.send(ByteBuffer.wrap(msg.toByteArray()), context);
        }
        r = MessageBuilder.buildResponse("Update");
        break;
      }
    } catch(Exception e) {
      //log.info(e);
      e.printStackTrace();
      throw e;
    }
    //log.info("r {}", r);
    return r;
  }

}
