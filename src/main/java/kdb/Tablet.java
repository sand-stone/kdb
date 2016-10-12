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

public class Tablet implements Closeable {
  private static Logger log = LogManager.getLogger(Tablet.class);
  private Connection conn;
  private String db;
  private static final String dbconfig = "create,cache_size=1GB,eviction=(threads_max=2,threads_min=2),lsm_manager=(merge=true,worker_thread_max=3), checkpoint=(log_size=2GB,wait=3600)";
  private Table table;

  public Tablet(String location, Table table) {
    Utils.checkDir(location);
    this.table = table;
    conn = wiredtiger.open(location, dbconfig);
    Session session = conn.open_session(null);
    log.info("storage: {}", getStorage());
    session.create("table:" + table.name, getStorage());
    session.close(null);
  }

  private String getColType(Table.Column col) {
    String ret = "";
    switch(col.getType()) {
    case Int8:
      ret = "b";
      break;
    case Int16:
      ret = "h";
      break;
    case Int32:
      ret = "i";
      break;
    case Int64:
      ret = "q";
      break;
    case Float:
      ret = "i";
      break;
    case Double:
      ret = "q";
      break;
    case Varchar:
      ret = "S";
      break;
    case Symbol:
      ret = "S";
      break;
    case Blob:
      ret = "u";
      break;
    case Timestamp:
      ret = "q";
      break;
    case DateTime:
      ret = "i";
      break;
    }
    return ret;
  }

  private String getStorage() {
    StringBuilder key_format = new StringBuilder();
    StringBuilder value_format = new StringBuilder();
    StringBuilder cols = new StringBuilder();
    key_format.append("key_format=");
    value_format.append(",value_format=");
    boolean haskey = false; boolean start = true;
    for(Table.Column col : table.cols) {
      if(col.iskey()) {
        key_format.append(getColType(col));
        haskey = true;
      } else {
        value_format.append(getColType(col));
      }
      if(!start) {
        cols.append(",");
      }
      start = false;
      cols.append(col.getName());
    }
    if(!haskey) {
      key_format.append("r");
      cols.insert(0,"columns=(id,");
    } else {
      cols.insert(0,"columns=(");
    }
    cols.append(")");
    return "(type=lsm," + key_format.toString() + "," + value_format.toString() + "," + cols + ")";
  }

  private int[] getPerm(List<String> names) {
    List<Table.Column> cols = table.getCols();
    int[] perm = new int[cols.size()];
    for(int i = 0; i < perm.length; i++) {
      int j = 0;
      for(String name: names) {
        if(cols.get(i).getName().equals(name)) {
          perm[i] = j;
          break;
        }
        j++;
      }
    }
    return perm;
  }

  public class Context implements Closeable {
    Session session;
    Cursor cursor;

    public Context() {
      session = Tablet.this.conn.open_session(null);
      cursor = session.open_cursor("table:" + Tablet.this.table.name, null, null);
    }

    public Context(String meta) {
      session = Tablet.this.conn.open_session(null);
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

  public void upsert(Context ctx, Message.UpsertTable msg) {
    log.info("cols: {}", msg.names);
    log.info("values: {}", msg.values);
    int[] perm = getPerm(msg.names);
    List<Object> values = msg.values;
    int len = Array.getLength(values.get(0));
    List<Table.Column> cols = table.getCols();
    ctx.cursor.reset();
    for(int i = 0; i < len; i++) {
      for(int j = 0; j < perm.length; j++) {
        Table.Column col = cols.get(j);
        switch(col.getType()) {
        case Int32:
          {
            int[] vals = (int[])values.get(perm[j]);
            log.info("put int: {}", vals[i]);
            if(col.iskey())
              ctx.cursor.putKeyInt(vals[i]);
            else
              ctx.cursor.putValueInt(vals[i]);
          }
          break;
        case Varchar:
          {
            String[] vals = (String[])values.get(perm[j]);
            log.info("put string: {}", vals[i]);
            if(col.iskey())
              ctx.cursor.putKeyString(vals[i]);
            else
              ctx.cursor.putValueString(vals[i]);
          }
          break;
        default:
          assert false: "unhandled type";
        }
      }
      ctx.cursor.insert();
    }
  }

  public void close() {
    conn.close(null);
  }

}
