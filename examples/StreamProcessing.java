import kdb.Client;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalTime;

public class StreamProcessing {

  private static String events = "xevents";
  private static String states = "xstates";

  private static boolean stop;
  private static String[] uris;

  private static UUID[] deviceIds;

  private static void init() {
    deviceIds = new UUID[15];
    for(int i = 0; i < deviceIds.length; i++) {
      deviceIds[i] = UUID.randomUUID();
    }
  }

  public static class EventSource implements Runnable {
    private int id;
    private Random rnd;
    private int valSize;

    public EventSource(int id) {
      this.id  = id;
      rnd = new Random();
      valSize = 300;
    }

    private void deviceid(ByteBuffer buf) {
      UUID guid = deviceIds[rnd.nextInt(deviceIds.length)];
      buf.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int batch = 0;
      int batchSize;
      int total = 0;
      long t1 = System.nanoTime();
      try(Client client = new Client(uris[0], events)) {
        int i = id;
        for(int j = 0; j < 12; j++) {
          for (int m = 0; m < deviceIds.length; m++) {
            for(int k = 0; k < 6; k++) {
              ByteBuffer key = ByteBuffer.allocate(19).order(ByteOrder.BIG_ENDIAN);
              key.put((byte)i);
              key.put((byte)j);
              UUID guid = deviceIds[m];
              key.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
              //deviceid(key);
              key.put((byte)k);
              keys.add(key.array());
              byte[] value = new byte[valSize];
              rnd.nextBytes(value);
              values.add(value);
            }
          }
          batchSize = keys.size();
          while(client.update(keys, values).status() != Client.Status.OK);
          long t2 = System.nanoTime();
          keys.clear();
          values.clear();
          total += batchSize;
          System.out.printf("eventsource %d, bucket %d:%d batchSize %d total %d events takes %e seconds, rate %e \n", id, i, j, batchSize, total,
                            (t2-t1)/1e9, total/((t2-t1)/1e9));
        }
      }
      System.out.printf("eventsource %d inserted %d events\n", id, total);
    }
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

    private void deviceid(ByteBuffer buf) {
      UUID guid = deviceIds[rnd.nextInt(deviceIds.length)];
      buf.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
    }

    private int process(Client.Result result) {
      Client client = new Client(uris[1], states);
      int ret = 0;
      if(result.count() <= 0)
        return ret;
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();

      for(int i = 0; i < numquery; i++) {
        for(int j = 0; j < result.count(); j++) {
          ByteBuffer key = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
          key.putShort((short)i);
          key.put(result.getKey(j), 2, 16);
          keys.add(key.array());
          byte[] payload = new byte[1];
          values.add(payload);
        }
      }
      client.update(keys, values);
      ret = values.size();
      client.close();
      //System.out.println("####" + result.count() + " ==> count: " + ret);
      return ret;
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      long t1 = System.nanoTime();
      int b1 = id;
      System.out.println("##### process bucket:" + b1);
      for(int b2 = 0; b2 < 12; b2++) {
        int count = 0;
        int ocount = 0;
        try (Client client = new Client(uris[0], events)) {
          ByteBuffer key1 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
          key1.put((byte)b1).put((byte)b2);
          ByteBuffer key2 = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
          key2.put((byte)(b1)).put((byte)(b2+1));
          Client.Result rsp = client.get(key1.array(), key2.array(), 100);
          ocount += rsp.count();
          count += process(rsp);
          while(rsp.token().length() > 0) {
            rsp = client.get(Client.QueryType.Between, rsp.token(), 100);
            ocount += rsp.count();
            count += process(rsp);
            System.out.println("ocount: " + ocount + " count: " + count);
          }
        }
        System.out.println("##### bucket " + b1 + ":" + b2 + " count: " + ocount + " xcount: " + count);
      }
    }
  }

  public static void main(String[] args) {
    if(args.length < 3) {
      System.out.println("Program http://localhost:8000/ http://localhost:8001/ http://localhost:8002/");
      return;
    }

    init();

    uris = args;
    System.out.println("start");
    System.out.println("create table");

    Client.createTable(uris[0], events);
    Client.createTable(uris[1], states);

    int num = 5;
    for (int i = 0; i < num; i++) {
      new Thread(new EventSource(i)).start();
    }

    System.out.println("event source threads");

    try {Thread.currentThread().sleep(5000);} catch(Exception ex) {}

    for (int i = 0; i < num; i++) {
      new Thread(new QueryState(i)).start();
    }

    try {Thread.currentThread().sleep(60*60*1000);} catch(Exception ex) {}
    stop = true;
  }
}
