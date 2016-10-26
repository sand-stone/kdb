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

public class XEvent {

  public static String table = "xevent";

  public static void createTable(Client client) {
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildCreateOp(table));
  }

  public static void dropTable(Client client) {
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildDropOp(table));
  }

  public static void addEvents(Client client) {
    int count = 10;
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
    for (int i = 0; i < count; i++) {
      keys.add(("key"+i).getBytes());
      values.add(("value-1-"+i).getBytes());
    }
    client.sendMsg("http://localhost:8000/", MessageBuilder.buildUpdateOp(table, keys, values));
  }

  public static void readEvents(Client client) {
    Message msg = client.sendMsg("http://localhost:8001/", MessageBuilder.buildGetOp(table, "key2".getBytes(), "key8".getBytes(), 10));
    System.out.println(msg);
  }

  public static void main(String[] args) {
    System.out.println("start");
    Client client = new Client();
    System.out.println("create table");
    createTable(client);
    try { Thread.currentThread().sleep(100); } catch(Exception e) {}
    System.out.println("add events");
    addEvents(client);
    try { Thread.currentThread().sleep(1000); } catch(Exception e) {}
    System.out.println("query events");
    readEvents(client);
    System.out.println("drop table");
    dropTable(client);
    System.out.println("close client");
    client.close();
    System.exit(0);
  }
}
