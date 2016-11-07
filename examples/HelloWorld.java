import kdb.Client;

import java.util.ArrayList;
import java.util.List;

public class HelloWorld {

  public static void main(String[] args) {
    List<byte[]> keys = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();

    for(int i = 0; i < 10; i++) {
      keys.add(("key"+i).getBytes());
      values.add(("value"+i).getBytes());
    }

    String table = "helloworld";
    Client.createTable("http://localhost:8000", table);
    try(Client client = new Client("http://localhost:8000", table)) {
      Client.Result r = client.insert(keys, values);
      System.out.println("r :" + r);
      r = client.get("key3".getBytes(), "key8".getBytes(), 20);
      System.out.println("r :" + r);
    }
    Client.dropTable("http://localhost:8000", table);
    System.exit(0);
  }
}
