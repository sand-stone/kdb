package kdb;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;
import kdb.rsm.ZabException;

final class DataNode {
  private static Logger log = LogManager.getLogger(DataNode.class);
  private Store store;
  private boolean standalone;
  private List<Ring> rings;
  private Random rnd;
  private ConcurrentHashMap<String, Store.Context> ctxs;
  private ConcurrentHashMap<String, Counters> counters;

  public static class Metric {
    int create;
    int drop;
    int get;
    int update;
    int insert;
  }

  public class Stats {
    public HashMap<String, Integer> sessions;
    public HashMap<String, Metric> metrics;

    public Stats() {
      sessions = new HashMap<String, Integer>();
      metrics = new HashMap<String, Metric>();
    }
  }

  static class Counters {
    AtomicInteger create;
    AtomicInteger drop;
    AtomicInteger get;
    AtomicInteger update;
    AtomicInteger insert;

    public Counters() {
      create = new AtomicInteger();
      drop = new AtomicInteger();
      get = new AtomicInteger();
      update = new AtomicInteger();
      insert = new AtomicInteger();
    }

    public void incrementCreate() {
      create.lazySet(create.get()+1);
    }

    public void incrementDrop() {
      drop.lazySet(drop.get()+1);
    }

    public void incrementGet() {
      get.lazySet(get.get()+1);
    }

    public void incrementUpdate() {
      update.lazySet(update.get()+1);
    }

    public void incrementInsert() {
      insert.lazySet(insert.get()+1);
    }
  }

  public DataNode(List<Ring> rings, Store store, boolean standalone) {
    this.rings = rings;
    this.store = store;
    this.standalone = standalone;
    this.ctxs = new ConcurrentHashMap<String, Store.Context>();
    this.counters = new ConcurrentHashMap<String, Counters>();
    this.rnd = new Random();
  }

  public Stats stats() {
    Stats stats = new Stats();
    //log.info("stats {}", store.tables);
    store.tables.forEach((k, v) -> stats.sessions.put(k, v.get()));
    counters.forEach((k, v) -> { Metric m = new Metric();
        m.create = v.create.get();
        m.drop = v.drop.get();
        m.get = v.get.get();
        m.update = v.update.get();
        m.insert = v.insert.get();
        stats.metrics.put(k, m); });
    return stats;
  }

  private void countCreate(String table) {
    Counters counts;
    if((counts = counters.get(table)) == null) {
      counts = new Counters();
      Counters old = counters.putIfAbsent(table, counts);
      if(old != null)
        counts = old;
    }
    counts.incrementCreate();
  }

  private void countDrop(String table) {
    Counters counts;
    if((counts = counters.get(table)) == null) {
      counts = new Counters();
      Counters old = counters.putIfAbsent(table, counts);
      if(old != null)
        counts = old;
    }
    counts.incrementDrop();
  }

  private void countGet(String table) {
    Counters counts;
    if((counts = counters.get(table)) == null) {
      counts = new Counters();
      Counters old = counters.putIfAbsent(table, counts);
      if(old != null)
        counts = old;
    }
    counts.incrementGet();
  }

  private void countUpdate(String table) {
    Counters counts;
    if((counts = counters.get(table)) == null) {
      counts = new Counters();
      Counters old = counters.putIfAbsent(table, counts);
      if(old != null)
        counts = old;
    }
    counts.incrementUpdate();
  }

  private void countInsert(String table) {
    Counters counts;
    if((counts = counters.get(table)) == null) {
      counts = new Counters();
      Counters old = counters.putIfAbsent(table, counts);
      if(old != null)
        counts = old;
    }
    counts.incrementInsert();
  }

  private Ring ring() {
    return rings.get(rnd.nextInt(rings.size()));
  }

  private void rsend(Message msg, Object ctx) {
    try {
      ring().zab.send(ByteBuffer.wrap(msg.toByteArray()), ctx);
    } catch(ZabException.InvalidPhase e) {
      throw new KdbException(e);
    } catch(ZabException.TooManyPendingRequests e) {
      throw new KdbException(e);
    }
  }

  public void process(Message msg, Object context) {
    Message r = MessageBuilder.nullMsg;
    String table;
    Store.Context ctx;
    //log.info("msg {} context {} standalone {}", msg, context, standalone);
    switch(msg.getType()) {
    case Create:
      table = msg.getCreateOp().getTable();
      countCreate(table);
      if(standalone) {
        r = store.create(table);
      } else {
        rsend(msg, context);
      }
      break;
    case Drop:
      //log.info("msg {} context {}", msg, context);
      table = msg.getDropOp().getTable();
      countDrop(table);
      if(standalone) {
        r = store.drop(table);
      } else {
        rsend(msg, context);
      }
      break;
    case Get:
      r = MessageBuilder.emptyMsg;
      String token = msg.getGetOp().getToken();
      //log.info("token <{}> ", token);
      if(token.equals("")) {
        table = msg.getGetOp().getTable();
        countGet(table);
        if(table != null && table.length() > 0)
          ctx = store.getContext(table);
        else {
          break;
        }
      } else {
        ctx = ctxs.get(token);
        if(ctx == null) {
          break;
        }
      }
      try {
        countGet(ctx.table);
        r = store.get(ctx, msg);
      } finally {
        if(ctx.done) {
          ctxs.remove(ctx.token());
          ctx.close();
        } else {
          ctxs.putIfAbsent(ctx.token(), ctx);
        }
      }
      break;
    case Insert:
      table = msg.getInsertOp().getTable();
      countInsert(table);
      if(standalone) {
        try(Store.Context c = store.getContext(table)) {
          r = store.insert(c, msg);
        }
      } else {
        rsend(msg, context);
      }
      break;
    case Update:
      table = msg.getUpdateOp().getTable();
      countUpdate(table);
      if(standalone) {
        try(Store.Context c = store.getContext(table)) {
          r = store.update(c, msg);
        }
      } else {
        rsend(msg, context);
      }
      break;
    }
    if(r != MessageBuilder.nullMsg) {
      NettyTransport.HttpKdbServerHandler.reply(context, r);
    }
  }

}
