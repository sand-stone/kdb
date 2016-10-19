package kdb;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import static spark.Spark.*;
import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;

import kdb.proto.XMessage.Message;
import kdb.proto.XMessage.InsertOperation;
import kdb.proto.XMessage.UpdateOperation;
import kdb.proto.XMessage.GetOperation;

public final class DataNode {
  private static Logger log = LogManager.getLogger(DataNode.class);

  public DataNode() {

  }

  public void run(PropertiesConfiguration config) {
    int maxThreads = 8;
    int minThreads = 2;
    int timeOutMillis = 30000;
    int port = config.getInt("port");
    Store store = new Store(config.getString("store"));
    boolean standalone = config.getBoolean("standalone", false);
    final Ring ring = new Ring(config.getString("ringaddr"), config.getString("leader"), config.getString("logDir"));;
    if(!standalone) {
      ring.bind(store);
    }
    port(port);
    threadPool(maxThreads, minThreads, timeOutMillis);
    get("/", (req, res) -> "kdb DataNode");
    post("/service", (request, response) -> {
        try {
          byte[] data = request.bodyAsBytes();
          Message msg = Message.parseFrom(data);
          String table;
          log.info("msg {}", msg);
          OutputStream os = response.raw().getOutputStream();
          switch(msg.getType()) {
          case Create:
            if(standalone) {
              table = msg.getCreateOp().getTable();
              store.create(table);
            } else {
              ring.zab.send(ByteBuffer.wrap(data), null);
            }
            os.write(MessageBuilder.buildResponse("Create").toByteArray());
            break;
          case Drop:
            table = msg.getDropOp().getTable();
            store.drop(table);
            os.write(MessageBuilder.buildResponse("Drop").toByteArray());
            break;
          case Get:
            byte[] ret = null;
            table = msg.getGetOp().getTable();
            try(Store.Context ctx = store.getContext(table)) {
              ret = store.get(ctx, msg);
            }
            if(ret != null) {
              os.write(MessageBuilder.buildResponse(ret).toByteArray());
            }
            break;
          case Insert:
            if(standalone) {
              table = msg.getInsertOp().getTable();
              try(Store.Context ctx = store.getContext(table)) {
                store.insert(ctx, msg);
              }
            } else {
              ring.zab.send(ByteBuffer.wrap(data), null);
            }
            os.write(MessageBuilder.buildResponse("Insert").toByteArray());
            break;
          case Update:
            if(standalone) {
              table = msg.getUpdateOp().getTable();
              try(Store.Context ctx = store.getContext(table)) {
                store.update(ctx, msg);
              }
            } else {
              ring.zab.send(ByteBuffer.wrap(data), null);
            }
            os.write(MessageBuilder.buildResponse("Update").toByteArray());
            break;
          }
          os.flush();
          os.close();
        } catch(Exception e) {
          e.printStackTrace();
          log.info(e.toString());
        }
        return "service return";
      });
    init();
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 1) {
      System.out.println("java -cp ./target/kdb-1.0-SNAPSHOT.jar kdb.DataNode conf/datanode.properties");
      return;
    }
    File propertiesFile = new File(args[0]);
    if(!propertiesFile.exists()) {
      System.out.printf("config file %s does not exist", propertiesFile.getName());
      return;
    }
    Configurations configs = new Configurations();
    PropertiesConfiguration config = configs.properties(propertiesFile);
    new DataNode().run(config);
  }

}
