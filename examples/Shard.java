import kdb.Client;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalTime;

public class Shard {

  private static String[] events;
  private static String[] states;

  private static boolean stop;
  private static String[] uris;

  private static UUID[] deviceIds;

  private static int numT = 6;

  private static void init() {
    //deviceIds = new UUID[150000000];
    //deviceIds = new UUID[100000];
    deviceIds = new UUID[1000000];
    for(int i = 0; i < deviceIds.length; i++) {
      deviceIds[i] = UUID.randomUUID();
    }
  }

  public static int memcmp(final byte[] a, final byte[] b, int len) {
    for (int i = 0; i < len; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);
      }
    }
    return 0;
  }

  public static class QueryState implements Runnable {
    private int id;
    private Random rnd;
    private int numquery;

    public QueryState(int id) {
      this.id = id;
      rnd = new Random();
      numquery = 7000;
    }

    private int process(Client client, Client.Result result) {
      if(result.count() <= 0)
        return 0;
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      byte[] k = null; int joincount = 0;
      for(int j = 0; j < result.count(); j++) {
        if(k == null || memcmp(k, result.getKey(j), 18) != 0) {
          k = result.getKey(j);
          ByteBuffer key = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
          key.put(result.getKey(j), 0, 18);
          keys.add(key.array());
          BitSet bits = new BitSet(numquery);
          bits.flip(0, bits.size());
          values.add(bits.toByteArray());
        } else {
          joincount++;
        }
      }
      //System.out.println("<<<<<" + values.size());
      client.update(keys, values);
      //System.out.println(">>>>");
      //System.out.println("####" + result.count() + " state count: " + values.size() + " join:" + joincount);
      return values.size();
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      long t1 = System.nanoTime();
      int b1 = id;
      Client statesClient = new Client(uris[0], states[id]);
      try (Client client = new Client(uris[0], events[id])) {
        int reprocessing = 2;
        while(reprocessing-- > 0) {
          int count = 0;
          int scount = 0;
          System.out.println("##### reprocessing "+ reprocessing + " start process bucket:" + id);
          ByteBuffer key1 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
          key1.putShort((short)0);
          ByteBuffer key2 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
          key2.putShort((short)0xFFFF);
          Client.Result rsp = client.get(key1.array(), key2.array(), 100);
          count += rsp.count();
          scount += process(statesClient, rsp);
          while(rsp.token().length() > 0) {
            rsp = client.get(Client.QueryType.Between, rsp.token(), 100);
            count += rsp.count();
            scount += process(statesClient, rsp);
          }
          //System.out.println("##### bucket " + b1 + ":" + b2 + " count: " + " scount: " + scount);
          System.out.println("##### reprocessing " + reprocessing + " done process bucket:" + id + " count: " + count + " scount: " + scount);
        }
      }
      statesClient.close();
    }

  }

  private static Random rnd = new Random();

  public static void generate(int id) {
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    int batch = 0;
    int batchSize;
    int total = 0;
    long t1 = System.nanoTime();
    try(Client client = new Client(uris[0], events[id])) {
      for (int m = 0; m < deviceIds.length; m++) {
        for(int k = 0; k < 6; k++) {
          ByteBuffer key = ByteBuffer.allocate(19).order(ByteOrder.BIG_ENDIAN);
          key.putShort((short)id);
          UUID guid = deviceIds[m];
          key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
          key.put((byte)k);
          keys.add(key.array());
          byte[] value = new byte[300];
          rnd.nextBytes(value);
          values.add(value);
        }
        if(m%100 == 0) {
          batchSize = keys.size();
          while(client.update(keys, values).status() != Client.Status.OK);
          keys.clear();
          values.clear();
          total += batchSize;
          long t2 = System.nanoTime();
          //System.out.printf("eventsource %d, bucket %d:%d batchSize %d total %d events takes %e seconds, rate %e \n",
          //                  id, i, j, batchSize, total,(t2-t1)/1e9, total/((t2-t1)/1e9));
        }
      }
    }
    System.out.printf("eventsource %d inserted %d events\n", id, total);
  }


  public static void main(String[] args) {
    if(args.length < 3) {
      System.out.println("Program http://localhost:8000/ http://localhost:8001/ http://localhost:8002/");
      return;
    }
    uris = args;
    init();

    events = new String[numT];
    states = new String[numT];
    for(int i = 0; i < numT; i++) {
      events[i] = "xevents"+i;
      Client.createTable(uris[0], events[i]);
      states[i] = "xstates"+i;
      Client.createTable(uris[0], states[i]);
    }

    for(int i = 0; i < numT; i++) {
      System.out.println("generate event table" + i);
      generate(i);
      try {Thread.currentThread().sleep(1000);} catch(Exception ex) {}
      System.out.println("process event table");
      new Thread(new QueryState(i)).start();
    }

    System.out.println("***** generate all event *****");
    try {Thread.currentThread().sleep(60*60*1000);} catch(Exception ex) {}
    stop = true;
  }
}
