import kdb.Client;
import kdb.MessageBuilder;
import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;
import kdb.proto.XMessage.DropOperation;

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
    deviceIds = new UUID[10];
    for(int i = 0; i < deviceIds.length; i++) {
      deviceIds[i] = UUID.randomUUID();
    }
  }

  public static class Writer implements Runnable  {
    private int id;
    private Random rnd;
    private int numQuery;

    public Writer(int id) {
      this.id  = id;
      rnd = new Random();
      numQuery = 1000;
    }

    private void bucketid(ByteBuffer buf) {
      LocalTime time = LocalTime.now();
      buf.put((byte)time.getHour());
      buf.put((byte)time.getMinute());
    }

    private void queryid(ByteBuffer buf) {
      buf.putInt(rnd.nextInt(numQuery));
    }

    private void deviceid(ByteBuffer buf) {
      UUID guid = deviceIds[rnd.nextInt(deviceIds.length)];
      buf.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
    }

    private void genData(List<byte[]> keys, List<byte[]> values) {
      int batch = 10;
      for (int i = 0; i < batch; i++) {
        ByteBuffer key = ByteBuffer.allocate(22).order(ByteOrder.BIG_ENDIAN);
        bucketid(key); queryid(key); deviceid(key);
        keys.add(key.array());
        values.add("value".getBytes());
      }
    }

    public void run() {
      List<byte[]> keys = new ArrayList<byte[]>();
      List<byte[]> values = new ArrayList<byte[]>();
      try(Client client = new Client(uris[0])) {
        while(!stop) {
          genData(keys, values);
          client.sendMsg(MessageBuilder.buildUpdateOp(table,
                                                      keys,
                                                      values));
          keys.clear();
          values.clear();
          //try {Thread.currentThread().sleep(100);} catch(Exception ex) {}
        }
      }
      System.out.printf("writer %d exit \n", id);
    }
  }

  public static class Reader implements Runnable  {
    private int id;

    public Reader(int id) {
      this.id = id;
    }

    public void run() {
      try (Client client = new Client(uris[1])) {
        while(!stop) {
          Message msg = client.sendMsg(MessageBuilder.buildGetOp(table,
                                                                 "key2".getBytes(),
                                                                 "key8".getBytes(),
                                                                 10));
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

    private void queryid(ByteBuffer buf, int id) {
      buf.putInt(id);
    }

    public void run() {
      try (Client client = new Client(uris[1]) ) {
        while(!stop) {
          ByteBuffer key = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
          bucketid(key, 0,0);
          Message msg = client.sendMsg(MessageBuilder.buildGetOp(table,
                                                                 GetOperation.Type.GreaterEqual,
                                                                 key.array(),
                                                                 10));
          System.out.println("msg:"+msg);
          msg = client.sendMsg(MessageBuilder.buildGetOp(msg.getResponse().getToken(), GetOperation.Type.Done, 0));

        }
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

    try (Client client = new Client(uris[0])) {
      client.sendMsg(MessageBuilder.buildCreateOp(table));
    }
    try { Thread.currentThread().sleep(100); } catch(Exception e) {}

    new Thread(new Writer(0)).start();
    System.out.println("start writer threads");

    new Thread(new Scanner()).start();
    System.out.println("start scanner threads");
    /*
      int nr = 3;
      for (int i= 0; i < nr; i++) {
      new Thread(new Reader(i)).start();
      }
      System.out.println("start reader threads");
    */
    try {Thread.currentThread().sleep(10000);} catch(Exception ex) {}
    stop = true;

    try {Thread.currentThread().sleep(3000);} catch(Exception ex) {}

    /*
      try (Client = new Client(uris[0])) {
      client.sendMsg(MessageBuilder.buildDropOp(table));
      }*/
    System.exit(0);
  }
}
