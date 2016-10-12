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

public class Metabase implements Closeable {
  private static Logger log = LogManager.getLogger(Metabase.class);
  private Connection conn;
  private String db;
  private static final String tnx = "isolation=snapshot";
  private final String dataDir = "./data";
  private final String metaDir = "./meta";
  private final String dbconfig = "create";

  public Metabase(String location) {
    boolean exists = Utils.checkDir(metaDir);
    conn = wiredtiger.open(metaDir, dbconfig);
    if(!exists) {
      Session session = conn.open_session(null);
      session.create("table:metabase", "key_format=qq,value_format=i,columns=(start,end,num)");
      session.checkpoint(null);
      session.close(null);
    }
  }

  public int getShards(int start) {
    int num = 1;
    Session session = conn.open_session(null);
    Cursor cursor = session.open_cursor("table:metabase", null, null);
    cursor.putKeyLong(start);
    if(cursor.search_near() == SearchStatus.LARGER) {
      do {
        if(cursor.getKeyLong()>start)
          num = cursor.getValueInt();        
      } while(cursor.next() == 0);
    }
    cursor.close();
    session.close(null);
    return num;
  }
  
  public void update(long start, long end, int num) {
    Session session = conn.open_session(null);
    session.begin_transaction(tnx);
    Cursor cursor = null;
    try {
      cursor = session.open_cursor("table:metabase", null, null);
      cursor.putKeyLong(start);
      cursor.putKeyLong(end);
      cursor.putValueInt(num);
      cursor.insert();
      session.commit_transaction(null);
      session.checkpoint(null);
    } catch(WiredTigerRollbackException e) {
      session.rollback_transaction(tnx);
    }
    if(cursor != null)
      cursor.close();
    session.close(null);
  }
  
  public void close() {
    conn.close(null);
  }

}
