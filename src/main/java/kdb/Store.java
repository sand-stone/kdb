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
import java.util.concurrent.ConcurrentHashMap;

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
  private ConcurrentHashMap<String, AtomicInteger> sessions;

  public Store(String location) {
    Utils.checkDir(location);
    conn = wiredtiger.open(location, dbconfig);
    sessions = new ConcurrentHashMap<String, AtomicInteger>();
  }

  public class Context implements Closeable {
    Session session;
    String table;
    Cursor cursor;

    public Context(String table) {
      this.table = table;
      session = Store.this.conn.open_session(null);
      cursor = session.open_cursor("table:"+table, null, null);
      if(sessions.get(table) == null) {
        sessions.putIfAbsent(table, new AtomicInteger());
      }
      int v = sessions.get(table).getAndIncrement();
      if(v < 0)
        throw new KdbException("table is dropped");
    }

    public void close() {
      cursor.close();
      //session.checkpoint(null);
      session.close(null);
      sessions.get(table).getAndDecrement();
    }
  }

  public Context getContext(String table) {
    return new Context(table);
  }

  public synchronized void create(String table) {
    if(sessions.get(table) != null) {
      //throw new KdbException("table existed");
      log.info("{} table already existed", table);
      return;
    }
    Session session = conn.open_session(null);
    session.create("table:"+table, "(type=lsm,key_format=u,value_format=u)");
    session.checkpoint(null);
    session.close(null);
  }

  public synchronized void drop(String table) {
    Session session = conn.open_session(null);
    if(sessions.get(table) == null) {
      sessions.putIfAbsent(table, new AtomicInteger(-1));
    }
    int count;
    AtomicInteger v = sessions.get(table);
    while(true) {
      while((count = v.get()) > 0) {
        Thread.currentThread().yield();
      }
      if(v.get() < 0 || v.compareAndSet(0, -1))
        break;
    }
    try {
      session.drop("table:"+table, null);
    } catch(WiredTigerException e) {
      log.info("{} table not exist", table);
    }
    session.close(null);
    sessions.remove(table);
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
      log.info("{} look for {}", this, new String(msg.getGetOp().getKey().toByteArray()));
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
    byte[] arr = new byte[data.remaining()];
    data.get(arr);
    Message msg = Message.parseFrom(arr);
    if(msg.getType() == MessageType.Insert) {
      String table = msg.getInsertOp().getTable();
      try(Store.Context ctx = getContext(table)) {
        insert(ctx, msg);
      }
    } else if (msg.getType() == MessageType.Update) {
      String table = msg.getUpdateOp().getTable();
      try(Store.Context ctx = getContext(table)) {
        update(ctx, msg);
      }
    } else if(msg.getType() == MessageType.Create) {
      String table = msg.getCreateOp().getTable();
      create(table);
    }
  }

  public void close() {
    conn.close(null);
  }

}
