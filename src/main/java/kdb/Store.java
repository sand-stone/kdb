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
  private static final String dbconfig = "create,session_max=5000,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3),checkpoint=(log_size=2GB,wait=300)";
  ConcurrentHashMap<String, AtomicInteger> tables;

  public Store(String location) {
    Utils.checkDir(location);
    conn = wiredtiger.open(location, dbconfig);
    tables = new ConcurrentHashMap<String, AtomicInteger>();
  }

  public class Context implements Closeable {
    private AtomicInteger counts = new AtomicInteger();
    private final static int Count = 5000;
    Session session;
    String table;
    Cursor cursor;
    boolean done;
    byte[] bound;

    public Context(String table) {
      this.table = table;
      if(counts.getAndIncrement() > 5000) {
        counts.getAndDecrement();
        throw new KdbException("too many sessions");
      }
      session = Store.this.conn.open_session(null);
      cursor = session.open_cursor("table:"+table, null, null);
      if(tables.get(table) == null) {
        tables.putIfAbsent(table, new AtomicInteger());
      }
      int v = tables.get(table).getAndIncrement();
      if(v < 0) {
        done = true;
        throw new KdbException("table is dropped");
      }
      done = false;
      bound = null;
    }

    public String token() {
      return this.toString();
    }

    public boolean done() {
      return done;
    }

    public void close() {
      cursor.close();
      session.close(null);
      tables.get(table).getAndDecrement();
      counts.getAndDecrement();
      done = true;
      bound = null;
    }
  }

  public Context getContext(String table) {
    return new Context(table);
  }

  public Message create(String table) {
    if(tables.get(table) != null) {
      //throw new KdbException("table existed");
      //log.info("{} table already existed", table);
      return MessageBuilder.buildResponse("table already existed:" + table);
    }

    Session session = conn.open_session(null);
    int r = session.create("table:"+table, "(type=lsm,key_format=u,value_format=u)");
    if(r == 0)
      session.close(null);
    else
      throw new KdbException("errro code:"+ r);
    return MessageBuilder.buildResponse("create " + table);
  }

  public Message drop(String table) {
    if(tables.get(table) == null)
      return MessageBuilder.buildResponse("table does not exist:" + table);

    AtomicInteger v = tables.get(table);
    //log.info("drop count {}", v);
    if(v.get() > 0) {
      return MessageBuilder.buildResponse("table active:" + table);
    }
    Session session = conn.open_session(null);
    try {
      session.drop("table:"+table, null);
    } catch(WiredTigerException e) {
      log.info("{} table not exist", table);
    }
    session.close(null);
    tables.remove(table);
    return MessageBuilder.buildResponse("drop " + table);
  }

  public Message insert(Context ctx, Message msg) {
    assert msg.getType() == MessageType.Insert;
    InsertOperation op = msg.getInsertOp();
    ctx.cursor.reset();
    int len = op.getKeysCount();
    if(len != op.getValuesCount())
      throw new RuntimeException("wrong length");
    //log.info("insert {}", msg);
    for(int i = 0; i < len; i++) {
      ctx.cursor.putKeyByteArray(op.getKeys(i).toByteArray());
      ctx.cursor.putValueByteArray(op.getValues(i).toByteArray());
      ctx.cursor.insert();
    }
    return MessageBuilder.buildResponse("inserted");
  }

  public Message update(Context ctx, Message msg) {
    assert msg.getType() == MessageType.Update;
    UpdateOperation op = msg.getUpdateOp();
    ctx.cursor.reset();
    int len = op.getKeysCount();
    if(len != op.getValuesCount()) {
      if(op.getValuesCount() != 0) {
        throw new KdbException("wrong length");
      }
      ByteBuffer counter = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
      for(int i = 0; i < len; i++) {
        ctx.cursor.putKeyByteArray(op.getKeys(i).toByteArray());
        if(ctx.cursor.search() == 0) {
          ctx.cursor.getValueByteArray(counter.array(), 0, 4);
          int c = counter.getInt()+1;
          counter.clear();
          counter.putInt(c);
          ctx.cursor.putValueByteArray(counter.array());
        } else {
          counter.putInt(1);
          ctx.cursor.putValueByteArray(counter.array());
        }
        ctx.cursor.putKeyByteArray(op.getKeys(i).toByteArray());
        ctx.cursor.update();
        ctx.cursor.reset();
        counter.clear();
      }
      return MessageBuilder.buildResponse("updated");
    }

    if(msg.getUpdateOp().getOverwrite()) {
      for(int i = 0; i < len; i++) {
        ctx.cursor.putKeyByteArray(op.getKeys(i).toByteArray());
        ctx.cursor.putValueByteArray(op.getValues(i).toByteArray());
        ctx.cursor.update();
        ctx.cursor.reset();
      }
    } else {
      ByteBuffer counter = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
      for(int i = 0; i < len; i++) {
        ctx.cursor.putKeyByteArray(op.getKeys(i).toByteArray());
        if(ctx.cursor.search() == 0) {
          byte[] oldv = ctx.cursor.getValueByteArray();
          counter.put(oldv, 0, 4);
          counter.flip();
          byte[] newv = ByteBuffer.allocate(oldv.length+op.getValues(i).size())
            .order(ByteOrder.BIG_ENDIAN)
            .putInt(counter.getInt() + 1)
            .put(oldv, 4, oldv.length - 4)
            .put(op.getValues(i).toByteArray())
            .array();
          ctx.cursor.putValueByteArray(newv);
          counter.clear();
        } else {
          byte[] v = op.getValues(i).toByteArray();
          ByteBuffer buf = ByteBuffer.allocate(v.length+4).order(ByteOrder.BIG_ENDIAN);
          buf.putInt(1).put(v);
          ctx.cursor.putValueByteArray(buf.array());
        }
        ctx.cursor.putKeyByteArray(op.getKeys(i).toByteArray());
        ctx.cursor.update();
        ctx.cursor.reset();
      }
    }
    return MessageBuilder.buildResponse("updated");
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

  private Message buildrange(Context ctx, int limit, int count) {
    byte[] key, value;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    if(count == -1) {
      while(--limit>0 && ctx.cursor.next() == 0) {
        key = ctx.cursor.getKeyByteArray();
        if(Utils.memcmp(key, ctx.bound) > 0)
          break;
        value = ctx.cursor.getValueByteArray();
        keys.add(key);
        values.add(value);
      }
    } else {
      ByteBuffer counter = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
      while(--limit>0 && ctx.cursor.next() == 0) {
        key = ctx.cursor.getKeyByteArray();
        if(Utils.memcmp(key, ctx.bound) > 0)
          break;
        value = ctx.cursor.getValueByteArray();
        counter.put(value, 0, 4);
        if(counter.getInt() >= count) {
          keys.add(key);
          values.add(value);
        }
        counter.clear();
      }
    }
    ctx.done = limit != 0? true : false;
    return MessageBuilder.buildResponse(ctx.done? "" : ctx.token(), keys, values);
  }

  public Message get(Context ctx, Message msg) {
    Message r = MessageBuilder.emptyMsg;
    byte[] key, value;
    SearchStatus status;
    assert msg.getType() == MessageType.Get;
    //log.info("get {}", msg);
    if(!msg.getGetOp().getToken().equals("")) {
      switch(msg.getGetOp().getOp()) {
      case GreaterEqual:
        r = buildfwd(ctx, msg.getGetOp().getLimit());
        break;
      case LessEqual:
        r = buildbkw(ctx, msg.getGetOp().getLimit());
        break;
      case Between:
        if(ctx.bound != null) {
          r = buildrange(ctx, msg.getGetOp().getLimit(), msg.getGetOp().getCount());
        }
        break;
      case Done:
        ctx.done = true;
        break;
      }
      return r;
    }
    ctx.cursor.reset();
    switch(msg.getGetOp().getOp()) {
    case Equal:
      //log.info("{} look for {}", this, new String(msg.getGetOp().getKey().toByteArray()));
      if(msg.getGetOp().getKey().size() > 0) {
        ctx.cursor.putKeyByteArray(msg.getGetOp().getKey().toByteArray());
        if(ctx.cursor.search() == 0) {
          key = ctx.cursor.getKeyByteArray();
          value = ctx.cursor.getValueByteArray();
          ctx.done = true;
          r = MessageBuilder.buildResponse(key, value);
        }
      }
      break;
    case Between:
      if(msg.getGetOp().getKey().size() > 0) {
        //log.info("{} look for {}", this, new String(msg.getGetOp().getKey().toByteArray()));
        //log.info("{} look for {}", this, new String(msg.getGetOp().getKey2().toByteArray()));
        int limit = msg.getGetOp().getLimit();
        ctx.cursor.putKeyByteArray(msg.getGetOp().getKey().toByteArray());
        status = ctx.cursor.search_near();
        if(status == SearchStatus.FOUND || status == SearchStatus.LARGER) {
          List<byte[]> keys = new ArrayList<byte[]>();
          List<byte[]> values = new ArrayList<byte[]>();
          byte[] k2 = msg.getGetOp().getKey2().toByteArray();
          int count = msg.getGetOp().getCount();
          if(count == -1) {
            do {
              key = ctx.cursor.getKeyByteArray();
              if(Utils.memcmp(key, k2) > 0)
                break;
              value = ctx.cursor.getValueByteArray();
              keys.add(key);
              values.add(value);
            } while(--limit>0 && ctx.cursor.next() == 0);
          } else {
            ByteBuffer counter = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
            do {
              key = ctx.cursor.getKeyByteArray();
              if(Utils.memcmp(key, k2) > 0)
                break;
              value = ctx.cursor.getValueByteArray();
              counter.put(value, 0, 4);
              counter.flip();
              if(counter.getInt() >= count) {
                keys.add(key);
                values.add(value);
              }
              counter.clear();
            } while(--limit>0 && ctx.cursor.next() == 0);
          }
          ctx.done = limit != 0? true : false;
          ctx.bound = k2;
          r = MessageBuilder.buildResponse(ctx.done? "" : ctx.token(), keys, values);
        }
      }
      break;
    case GreaterEqual:
      //log.info("{} look for {}", this, new String(msg.getGetOp().getKey().toByteArray()));
      ctx.cursor.putKeyByteArray(msg.getGetOp().getKey().toByteArray());
      status = ctx.cursor.search_near();
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
      break;
    case LessEqual:
      //log.info("{} look for {}", this, new String(msg.getGetOp().getKey().toByteArray()));
      ctx.cursor.putKeyByteArray(msg.getGetOp().getKey().toByteArray());
      status = ctx.cursor.search_near();
      if(status == SearchStatus.FOUND || status == SearchStatus.SMALLER) {
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
        } while(--limit>0 && ctx.cursor.prev() == 0);
        ctx.done = limit != 0? true : false;
        r = MessageBuilder.buildResponse(ctx.done? "" : ctx.token(), keys, values);
      }
      break;
    default:
      break;
    }
    return r;
  }

  public Message handle(ByteBuffer data) throws IOException {
    byte[] arr = new byte[data.remaining()];
    data.get(arr);
    Message msg = Message.parseFrom(arr);
    //log.info("handle {}", msg);
    if(msg.getType() == MessageType.Insert) {
      String table = msg.getInsertOp().getTable();
      try(Store.Context ctx = getContext(table)) {
        msg = insert(ctx, msg);
      }
    } else if (msg.getType() == MessageType.Update) {
      String table = msg.getUpdateOp().getTable();
      try(Store.Context ctx = getContext(table)) {
        msg = update(ctx, msg);
      }
    } else if(msg.getType() == MessageType.Create) {
      String table = msg.getCreateOp().getTable();
      msg = create(table);
    } else if(msg.getType() == MessageType.Drop) {
      String table = msg.getDropOp().getTable();
      msg = drop(table);
    }
    return msg;
  }

  public void close() {
    conn.close(null);
  }

}
