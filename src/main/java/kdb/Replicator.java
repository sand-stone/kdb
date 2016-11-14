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
import org.asynchttpclient.*;
import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.Message.MessageType;

class Replicator implements Closeable, Runnable {
  private static Logger log = LogManager.getLogger(Replicator.class);
  private Store store;
  private ConcurrentHashMap<String, Store.Lsn> lsns;
  private LinkedBlockingQueue<Message> replQ;
  private boolean stop;

  public Replicator(Store store) {
    this.store = store;
    lsns = new ConcurrentHashMap<String, Store.Lsn>();
    replQ = new LinkedBlockingQueue<Message>();
    stop = false;
  }

  public void addLogReq(Message msg, long epoch, long xid) {
    do {
      try {
        Store.Lsn lsn = lsns.get(msg.getLogOp().getUri());
        int file = 0;
        long offset = 0;
        if(lsn != null) {
          file = lsn.file;
          offset = lsn.offset;
        }
        Message req = MessageBuilder.buildLogOp(msg, epoch, xid, file, offset);
        replQ.put(req);
        log.info("enq req {}", req);
        break;
      } catch (InterruptedException e) {}
    } while(true);
  }

  public void run() {
    log.info("repl starts");
    final AsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(10000).build();
    AsyncHttpClient client = new DefaultAsyncHttpClient(config);
    while(!stop) {
      Message msg = null;
      try {
        msg = replQ.peek();
        if(msg != null) {
          log.info("process req {}", msg);
          String uri = msg.getLogOp().getUri();
          Response r = client
            .preparePost(uri)
            .setBody(msg.toByteArray())
            .execute()
            .get();
          byte[] data = r.getResponseBodyAsBytes();
          msg = Message.parseFrom(data);
          store.applyLog(msg);
          Store.Lsn lsn = new Store.Lsn();
          lsn.file = msg.getResponse().getLsnfile();
          lsn.offset = msg.getResponse().getLsnoffset();
          lsns.put(uri, lsn);
          replQ.take();
        }
        Thread.yield();
      } catch(Exception e) {
        log.info(e);
      }
    }
  }

  public void close() {
    stop = true;
    try {
      Thread.currentThread().sleep(1000);
    } catch (InterruptedException e) {}
  }

}
