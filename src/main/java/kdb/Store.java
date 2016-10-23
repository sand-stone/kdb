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
    boolean done;

    public Context(String table) {
      this.table = table;
      session = Store.this.conn.open_session(null);
      cursor = session.open_cursor("table:"+table, null, null);
      if(sessions.get(table) == null) {
        sessions.putIfAbsent(table, new AtomicInteger());
      }
      int v = sessions.get(table).getAndIncrement();
      if(v < 0) {
        done = true;
        throw new KdbException("table is dropped");
      }
      done = false;
    }

    public String token() {
      return this.toString();
    }

    public boolean done() {
      return done;
    }

    public void close() {
      cursor.close();
      //session.checkpoint(null);
      session.close(null);
      sessions.get(table).getAndDecrement();
      done = true;
    }
  }

  public Context getContext(String table) {
    return new Context(table);
  }

  public synchronized void create(String table) {
    if(sessions.get(table) != null) {
      //throw new KdbException("table existed");
      //log.info("{} table already existed", table);
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

  private Message buildfwd(Context ctx, int limit) {
    byte[] key, value;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    while(--limit>0 && ctx.cursor.next() == 0) {
      key = ctx.cursor.getKeyByteArray();
      value = ctx.cursor.getValueByteArray();
      keys.add(key);
      values.add(value);
    }
    ctx.done = limit != 0? true : false;
    return MessageBuilder.buildResponse(ctx.done? "" : ctx.token(), keys, values);
  }

  private Message buildbkw(Context ctx, int limit) {
    byte[] key, value;
    Message r = MessageBuilder.nullMsg;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    while(--limit>0 && ctx.cursor.prev() == 0) {
      key = ctx.cursor.getKeyByteArray();
      value = ctx.cursor.getValueByteArray();
      keys.add(key);
      values.add(value);
    }
    ctx.done = limit != 0? true : false;
    return MessageBuilder.buildResponse(ctx.done? "" : ctx.token(), keys, values);
  }

  public Message get(Context ctx, Message msg) {
    Message r = MessageBuilder.nullMsg;
    byte[] key, value;
    assert msg.getType() == MessageType.Get;
    if(!msg.getGetOp().getToken().equals("")) {
      switch(msg.getGetOp().getOp()) {
      case GreaterEqual:
        r = buildfwd(ctx, msg.getGetOp().getLimit());
        break;
      case LessEqual:
        r = buildbkw(ctx, msg.getGetOp().getLimit());
        break;
      case None:
        ctx.done = true;
        r = MessageBuilder.emptyMsg;
        break;
      }
      return r;
    }
    ctx.cursor.reset();
    switch(msg.getGetOp().getOp()) {
    case Equal: {
      //log.info("{} look for {}", this, new String(msg.getGetOp().getKey().toByteArray()));
      ctx.cursor.putKeyByteArray(msg.getGetOp().getKey().toByteArray());
      if(ctx.cursor.search() == 0) {
        key = ctx.cursor.getKeyByteArray();
        value = ctx.cursor.getValueByteArray();
        ctx.done = true;
        r = MessageBuilder.buildResponse(key, value);
      }
    }
      break;
    case GreaterEqual: {
      //log.info("{} look for {}", this, new String(msg.getGetOp().getKey().toByteArray()));
      ctx.cursor.putKeyByteArray(msg.getGetOp().getKey().toByteArray());
      SearchStatus status = ctx.cursor.search_near();
      if(status == SearchStatus.FOUND || status == SearchStatus.LARGER) {
        int limit = msg.getGetOp().getLimit();
        //log.info("limit {}", limit);
        List<byte[]> keys = new ArrayList<byte[]>();
        List<byte[]> values = new ArrayList<byte[]>();
        do {
          key = ctx.cursor.getKeyByteArray();
          value = ctx.cursor.getValueByteArray();
          keys.add(key);
          values.add(value);
          //log.info("key {} value {} ", new String(key), new String(value));
        } while(--limit>0 && ctx.cursor.next() == 0);
        ctx.done = limit != 0? true : false;
        r = MessageBuilder.buildResponse(ctx.done? "" : ctx.token(), keys, values);
      }
    }
      break;
    case LessEqual: {
      log.info("{} look for {}", this, new String(msg.getGetOp().getKey().toByteArray()));
      ctx.cursor.putKeyByteArray(msg.getGetOp().getKey().toByteArray());
      SearchStatus status = ctx.cursor.search_near();
      if(status == SearchStatus.FOUND || status == SearchStatus.SMALLER) {
        int limit = msg.getGetOp().getLimit();
        log.info("limit {}", limit);
        List<byte[]> keys = new ArrayList<byte[]>();
        List<byte[]> values = new ArrayList<byte[]>();
        do {
          key = ctx.cursor.getKeyByteArray();
          value = ctx.cursor.getValueByteArray();
          keys.add(key);
          values.add(value);
          log.info("key {} value {} ", new String(key), new String(value));
        } while(--limit>0 && ctx.cursor.prev() == 0);
        ctx.done = limit != 0? true : false;
        r = MessageBuilder.buildResponse(ctx.done? "" : ctx.token(), keys, values);
      }
    }
    break;
    default:
      break;
  }
  return r;
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
