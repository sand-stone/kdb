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

public class Shard implements Closeable {
  private static Logger log = LogManager.getLogger(Shard.class);
  private Connection conn;
  private String db;
  private static final String dbconfig = "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3), checkpoint=(log_size=2GB,wait=3600)";

  public Shard(String location) {
    Utils.checkDir(location);
    conn = wiredtiger.open(location, dbconfig);
    Session session = conn.open_session(null);
    session.create("table:kdb", getStorage());
    session.close(null);
  }

  private String getStorage() {
    return "(type=lsm,key_format=u,value_format=u)";
  }

  public class Context implements Closeable {
    Session session;
    Cursor cursor;

    public Context() {
      session = Shard.this.conn.open_session(null);
      cursor = session.open_cursor("table:kdb", null, null);
    }

    public Context(String meta) {
      session = Shard.this.conn.open_session(null);
      cursor = session.open_cursor("metadata:", null, null);
    }

    public void close() {
      cursor.close();
      session.checkpoint(null);
      session.close(null);
    }
  }

  public Context getContext() {
    return new Context();
  }

  public Context getContext(String meta) {
    return new Context(meta);
  }

  public void upsert(Context ctx, Message.Upsert msg) {
    log.info("cols: {}", msg.keys);
    log.info("values: {}", msg.values);
    ctx.cursor.reset();
    ctx.cursor.putKeyByteArray(null);
    ctx.cursor.putValueByteArray(null);
    ctx.cursor.insert(); //ctx.cursor.update();    
  }

  public void close() {
    conn.close(null);
  }

}
