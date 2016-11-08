import kdb.Client;

import java.util.ArrayList;
import java.util.List;

public class HelloWorld {

  public static void main(String[] args) {
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();

    String table = "helloworld";
    Client.createTable("http://localhost:8000", table);
    int start = 0; int batch = 10;
    boolean stop = false;
    while(!stop) {
      for(int i = 0; i < batch; i++) {
        keys.add(("key"+ (start + i)).getBytes());
        values.add(("value"+ (start + i)).getBytes());
      }
      try(Client client = new Client("http://localhost:8000", table)) {
        Client.Result r = client.insert(keys, values);
        System.out.println("r :" + r);
        //r = client.get(("key"+start).getBytes(), ("key"+(start+5)).getBytes(), 20);
        System.out.println("r :" + r);
      }

      boolean s = false;
      do {
        try {
          try(Client client = new Client("http://localhost:8002", table)) {
            Client.Result r = client.get(Client.QueryType.GreaterEqual, "key".getBytes(), 100);
            System.out.println("r :" + r);
            s = true;
            break;
          }
        } catch(Exception e) {
          System.out.println(e.getMessage());
        }
        try { Thread.currentThread().sleep(2000); } catch (InterruptedException e) {}
      } while(!s);
      keys.clear(); values.clear();
      start += batch;
      System.out.println("start :" + start);
      try {
        Thread.currentThread().sleep(2000);
        if(start > 60)
          stop = true;
      } catch (InterruptedException e) {}
    }
    Client.dropTable("http://localhost:8000", table);
    System.exit(0);
  }
}
