import kdb.Client;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalTime;

public class XEventPerf {

  private static String table = "xevent";
  private static boolean stop;
  private static String[] uris;

  private static UUID[] deviceIds;

  private static void init() {
    deviceIds = new UUID[15000000];
    for(int i = 0; i < deviceIds.length; i++) {
      deviceIds[i] = UUID.randomUUID();
    }
  }

  public static class Writer implements Runnable  {
    private int id;
    private Random rnd;
    private int batchSize;

    public Writer(int id) {
      this.id  = id;
      rnd = new Random();
      batchSize = 1000;
    }

    private void bucketid(ByteBuffer buf) {
      //LocalTime time = LocalTime.now();
      //buf.put((byte)time.getHour());
      //buf.put((byte)time.getMinute());
      buf.put((byte)rnd.nextInt(24)); //24 hour
      buf.put((byte)rnd.nextInt(12)); //every hour 12 buckets, each bucket is 5 mins
    }

    private void deviceid(ByteBuffer buf) {
      UUID guid = deviceIds[rnd.nextInt(deviceIds.length)];
      buf.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
    }

    private void genData(List<byte[]> keys, List<byte[]> values) {
      for (int i = 0; i < batchSize; i++) {
        ByteBuffer key = ByteBuffer.allocate(18).order(ByteOrder.BIG_ENDIAN);
        bucketid(key); deviceid(key);
        keys.add(key.array());
        //values.add(("[value#"+id+"#]").getBytes());
        byte[] payload = new byte[1228/*1024*1.2*/];
        rnd.nextBytes(payload);
      }
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      int batch = 0;
      try(Client client = new Client(uris[0], table)) {
        while(!stop) {
          genData(keys, values);
          long t1 = System.nanoTime();
          Client.Result rsp = client.append(keys, values);
          long t2 = System.nanoTime();
          keys.clear();
          values.clear();
          batch++;
          System.out.printf(" insert %d takes %e seconds \n", batchSize, (t2-t1)/1e9);
        }
      }
      System.out.printf("writer %d inserted %d events\n", id, batch*batchSize);
    }
  }

  public static class Reader implements Runnable  {
    private int id;

    public Reader(int id) {
      this.id = id;
    }

    public void run() {
      try (Client client = new Client(uris[1], table)) {
        while(!stop) {
        }
      }
      System.out.printf("reader %d exit \n", id);
    }
  }


  public static class Scanner implements Runnable  {
    public Scanner() {

    }

    private void bucketid(ByteBuffer buf, int hour, int minute) {
      buf.put((byte)hour);
      buf.put((byte)minute);
    }

    public void run() {
        while(!stop) {
          try (Client client = new Client(uris[2], table)) {
            ByteBuffer key = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
            bucketid(key, 0, 0);
            Client.Result rsp = client.get(Client.QueryType.GreaterEqual, key.array(), 100);
          }
      }
    }
  }

  public static class Counter implements Runnable  {
    String uri;

    public Counter(String uri) {
      this.uri = uri;
    }

    private void bucketid(ByteBuffer buf, int hour, int minute) {
      buf.put((byte)hour);
      buf.put((byte)minute);
    }

    public void run() {
      try (Client client = new Client(uri, table)) {
        ByteBuffer key = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
        bucketid(key, 0, 0);
        Client.Result rsp = client.get(Client.QueryType.GreaterEqual, key.array(), 100);
        int count = rsp.count();
        //System.out.println("msg:"+msg.getResponse().getKeysCount());
        while(rsp.token().length() > 0) {
          rsp = client.get(Client.QueryType.GreaterEqual, rsp.token(), 100);
          count += rsp.count();
          //System.out.println("msg:"+msg.getResponse().getKeysCount());
        }
        System.out.printf("total # msg %d \n", count);
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

    Client.createTable(uris[0], table);

    int nw = 3;
    for (int i = 0; i < nw; i++) {
      new Thread(new Writer(i)).start();
    }

    System.out.println("start writer threads");

    //new Thread(new Scanner()).start();
    //System.out.println("start scanner threads");
    /*
      int nr = 3;
      for (int i= 0; i < nr; i++) {
      new Thread(new Reader(i)).start();
      }
      System.out.println("start reader threads");
    */
    try {Thread.currentThread().sleep(5*60*1000);} catch(Exception ex) {}

    stop = true;

    try {Thread.currentThread().sleep(3000);} catch(Exception ex) {}

    System.out.println("start counter threads");
    new Thread(new Counter(uris[1])).start();
    new Thread(new Counter(uris[2])).start();
    new Thread(new Counter(uris[0])).start();

    try {Thread.currentThread().sleep(40000);} catch(Exception ex) {}

    System.out.println("start counter threads");
    new Thread(new Counter(uris[1])).start();
    new Thread(new Counter(uris[2])).start();
    new Thread(new Counter(uris[0])).start();

    while(true) {
      try {Thread.currentThread().sleep(10000);} catch(Exception ex) {}
    }
    //System.exit(0);
  }
}
