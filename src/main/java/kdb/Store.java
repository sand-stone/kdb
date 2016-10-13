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

  public void insert(Context ctx, Message.Insert msg) {
    log.info("cols: {}", msg.keys);
    log.info("values: {}", msg.values);
    ctx.cursor.reset();
    int len = msg.keys.size();
    if(len != msg.values.size())
      throw new RuntimeException("wrong length");
    for(int i = 0; i < len; i++) {
      ctx.cursor.putKeyByteArray(msg.keys.get(i));
      ctx.cursor.putValueByteArray(msg.values.get(i));
      ctx.cursor.insert();
    }
  }

  public byte[] get(Context ctx, Message.Get msg) {
    byte[] ret = null;
    ctx.cursor.reset();
    switch(msg.op) {
    case Equal: {
      ctx.cursor.putKeyByteArray(msg.key);
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

  public void handle(ByteBuffer msg) {
    Object o = Serializer.deserialize(msg);
    if(o instanceof Message.Insert) {
      try(Store.Context ctx = getContext()) {
        insert(ctx, (Message.Insert)o);
      }
    } else if (o instanceof Message.Upsert) {
      try(Store.Context ctx = getContext()) {
        upsert(ctx, (Message.Upsert)o);
      }
    }
  }

  public void upsert(Context ctx, Message.Upsert msg) {
    log.info("cols: {}", msg.keys);
    log.info("values: {}", msg.values);
    ctx.cursor.reset();
    ctx.cursor.putKeyByteArray(null);
    ctx.cursor.putValueByteArray(null);
    ctx.cursor.update();
  }

  public void close() {
    conn.close(null);
  }

}
