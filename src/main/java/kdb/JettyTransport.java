package kdb;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import kdb.proto.XMessage.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import kdb.rsm.ZabException;
import java.io.File;
import java.io.OutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public final class JettyTransport {
  private static final Logger log = LogManager.getLogger(JettyTransport.class);

  private JettyTransport() {}

  public static void reply(Object ctx, Message msg) {
    AsyncContext context = (AsyncContext)ctx;
    if (context == null) {
      // This request is sent from other instance.
      return;
    }
    //log.info("ctx {}", ctx);
    HttpServletResponse response =
      (HttpServletResponse)(context.getResponse());
    try {
      OutputStream os =response.getOutputStream();
      os.write(msg.toByteArray());
    } catch(IOException e) {
      log.info(e);
    } finally {
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      context.complete();
    }
  }

  static List<Ring> configRings(PropertiesConfiguration config, boolean standalone, Store store) {
    List ringaddrs = config.getList("ringaddr");
    List leaders = config.getList("leader");
    List logs = config.getList("logDir");

    int len = ringaddrs.size();
    if((leaders.size() > 0 && len != leaders.size()) || len != logs.size())
      throw new KdbException("ring config error");

    List<Ring> rings = new ArrayList<Ring>();
    for(int i = 0; i < len; i++) {
      Ring ring = new Ring((String)ringaddrs.get(i), leaders.size() == 0? null: (String)leaders.get(i), (String)logs.get(i));
      rings.add(ring);
      if(!standalone) {
        ring.bind(store);
      }
    }
    return rings;
  }

  public void start(PropertiesConfiguration config) throws Exception {
    int port = config.getInt("port");
    boolean SSL = config.getBoolean("ssl", false);

    Store store = new Store(config.getString("store"));
    boolean standalone = config.getBoolean("standalone", false);

    DataNode db = new DataNode(configRings(config, standalone, store), store, standalone);

    Server server = new Server(port);
    ServletHandler handler = new ServletHandler();
    server.setHandler(handler);
    // Handlers with the initialization order >= 0 get initialized on startup.
    // If you don't specify this, Zab doesn't get initialized until the first
    // request is received.
    ServletHolder holder = new ServletHolder(new KdbRequestHandler(db));
    handler.addServletWithMapping(holder, "/*");
    server.start();
    server.join();
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 1) {
      System.out.println("java -cp ./target/kdb-1.0-SNAPSHOT.jar kdb.HttpTransport conf/datanode.properties");
      return;
    }
    File propertiesFile = new File(args[0]);
    if(!propertiesFile.exists()) {
      System.out.printf("config file %s does not exist", propertiesFile.getName());
      return;
    }
    Configurations configs = new Configurations();
    PropertiesConfiguration config = configs.properties(propertiesFile);

    new JettyTransport().start(config);
  }


}
