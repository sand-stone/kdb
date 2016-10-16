package kdb;

import java.nio.*;
import java.io.*;
import java.lang.reflect.Array;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.google.gson.*;
import java.util.*;
import java.util.stream.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;

class Utils {
  private static Logger log = LogManager.getLogger(Utils.class);

  public static boolean checkDir(String dir) {
    File d = new File(dir);
    boolean ret = d.exists();
    if(ret && d.isFile())
      throw new RuntimeException("wrong directory:" + dir);
    if(!ret) {
      d.mkdirs();
    }
    return ret;
  }

  public static ByteBuffer serialize(Object msg) {
    try {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
           ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(msg);
        oos.close();
        return ByteBuffer.wrap(bos.toByteArray());
      }
    } catch(IOException e) {}
    return null;
  }

  public static Object deserialize(byte[] data) {
    return deserialize(ByteBuffer.wrap(data));
  }

  public static Object deserialize(ByteBuffer bb) {
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      return ois.readObject();
    } catch (ClassNotFoundException|IOException ex) {
      log.error("Failed to deserialize: {}", bb, ex);
      throw new RuntimeException("Failed to deserialize ByteBuffer");
    }
  }

}
