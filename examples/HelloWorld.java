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
import java.nio.*;

public class HelloWorld {

  public static void main(String[] args) {
    List<byte[]> keys = Arrays.asList("key1".getBytes(), "testKey".getBytes());
    List<byte[]> values = Arrays.asList("val1".getBytes(), "testvalue".getBytes());
    Client client = new Client();
    String table = "helloworld";
    Message msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildCreateOp(table));
    System.out.println("create:" + msg);
    try { Thread.currentThread().sleep(500); } catch(Exception e) {}
    msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildInsertOp(table, keys, values));
    System.out.println("insert:" + msg);
    try { Thread.currentThread().sleep(500); } catch(Exception e) {}
    msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildGetOp(table, GetOperation.Type.Equal, "testKey".getBytes()));
    System.out.println("get:" + msg);
    msg = client.sendMsg("http://localhost:8000/", MessageBuilder.buildDropOp(table));
    System.out.println("drop:" + msg);
    System.exit(0);
  }
}
