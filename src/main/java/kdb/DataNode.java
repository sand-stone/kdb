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

public final class DataNode {
  private static Logger log = LogManager.getLogger(DataNode.class);

  private static final String rootData = "./datanode";

  public static void main(String[] args) {
    int maxThreads = 8;
    int minThreads = 2;
    int timeOutMillis = 30000;
    int port = 8000;
    port(port);
    threadPool(maxThreads, minThreads, timeOutMillis);
    get("/", (req, res) -> "kdb DataNode");
    post("/createtable", (request, response) -> {
        try {
          byte[] data = request.bodyAsBytes();
          Message.CreateTable msg = (Message.CreateTable)Serializer.deserialize(data);
          log.info("msg {}", msg);
          return "create table\n";
        } catch(Exception e) {
          e.printStackTrace();
          log.info(e.toString());
          throw e;
        }
      });
    post("/upsertable", (request, response) -> {
        try {
          byte[] data = request.bodyAsBytes();
          Message.UpsertTable msg = (Message.UpsertTable)Serializer.deserialize(data);
          log.info("msg {}", msg);
          String table = request.queryParams("table");
          return "upsert table";
        }  catch(Exception e) {
          e.printStackTrace();
          log.info(e.toString());
          throw e;
        }
      });
    post("/deletetable", (request, response) -> {
        byte[] data = request.bodyAsBytes();
        Message.DeleteTable msg = (Message.DeleteTable)Serializer.deserialize(data);
        log.info("msg {}", msg);
        return "delete table\n";
      });
    post("/querytable", (request, response) -> {
        byte[] data = request.bodyAsBytes();
        Message.QueryTable msg = (Message.QueryTable)Serializer.deserialize(data);
        log.info("msg {}", msg.table);
        return "query table\n";
      });
    init();
  }

}
