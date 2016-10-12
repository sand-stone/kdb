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

}
