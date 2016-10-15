package kdb;

import com.wiredtiger.db.*;
import java.nio.*;
import java.io.*;
import java.lang.reflect.Array;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;
import kdb.proto.XMessage.Message.MessageType;

public class Store implements Closeable {
  private static Logger log = LogManager.getLogger(Store.class);
  private Connection conn;
  private String db;
  private static final String dbconfig = "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3),checkpoint=(log_size=2GB,wait=3600)";

  public Store(String location) {
    Utils.checkDir(location);
    conn = wiredtiger.open(location, dbconfig);
    Session session = conn.open_session(null);
    session.create("table:kdb", "(type=lsm,key_format=u,value_format=u)");
    session.close(null);
  }

  public class Context implements Closeable {
    Session session;
    Cursor cursor;

    public Context() {
      session = Store.this.conn.open_session(null);
      cursor = session.open_cursor("table:kdb", null, null);
    }

    public void close() {
      cursor.close();
      //session.checkpoint(null);
      session.close(null);
    }
  }

  public Context getContext() {
    return new Context();
  }

  public void insert(Context ctx, Message msg) {
    assert msg.getType() == MessageType.Insert;
    InsertOperation op = msg.getInsertOp();
    ctx.cursor.reset();
    int len = op.getKeysCount();
    if(len != op.getValuesCount())
      throw new RuntimeException("wrong length");
    for(int i = 0; i < len; i++) {
      ctx.cursor.putKeyByteArray(op.getKeys(i).toByteArray());
      ctx.cursor.putValueByteArray(op.getValues(i).toByteArray());
      ctx.cursor.insert();
    }
  }

  public void update(Context ctx, Message msg) {
    assert msg.getType() == MessageType.Update;
    UpdateOperation op = msg.getUpdateOp();
    ctx.cursor.reset();
    int len = op.getKeysCount();
    if(len != op.getValuesCount())
      throw new RuntimeException("wrong length");
    for(int i = 0; i < len; i++) {
      ctx.cursor.putKeyByteArray(op.getKeys(i).toByteArray());
      ctx.cursor.putValueByteArray(op.getValues(i).toByteArray());
      ctx.cursor.update();
    }
  }

  public byte[] get(Context ctx, Message msg) {
    byte[] ret = null;
    assert msg.getType() == MessageType.Get;
    ctx.cursor.reset();
    switch(msg.getGetOp().getOp()) {
    case Equal: {
      ctx.cursor.putKeyByteArray(msg.getGetOp().getKey().toByteArray());
      if(ctx.cursor.search() == 0) {
        log.info("found");
        ret = ctx.cursor.getValueByteArray();
      }
    }
      break;
    default:
      break;
    }
    return ret;
  }

  public void handle(ByteBuffer data) throws IOException {
    Message msg = Message.parseFrom(data.array());
    if(msg.getType() == MessageType.Insert) {
      try(Store.Context ctx = getContext()) {
        insert(ctx, msg);
      }
    } else if (msg.getType() == MessageType.Update) {
      try(Store.Context ctx = getContext()) {
        update(ctx, msg);
      }
    }
  }

  public void close() {
    conn.close(null);
  }

}
