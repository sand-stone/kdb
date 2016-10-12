package kdb;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public final class Message implements Serializable {
  private static Logger log = LogManager.getLogger(Message.class);

  public static final class Create implements Serializable {
    public int nshards;
    public Create(int nshards) {
      this.nshards = nshards;
    }
  }

  public static final class Insert implements Serializable {
    public List<byte[]> keys;
    public List<byte[]> values;

    public Insert(List<byte[]> keys, List<byte[]> values) {
      this.keys = keys;
      this.values = values;
    }
  }

  public static final class Upsert implements Serializable {
    public List<byte[]> keys;
    public List<byte[]> values;

    public Upsert(List<byte[]> keys, List<byte[]> values) {
      this.keys = keys;
      this.values = values;
    }
  }

  public static final class Get implements Serializable {
    public byte[] key;
    public Get(byte[] key) {
      this.key = key;
    }
  }

  public static final class MultiGet implements Serializable {
    public List<byte[]> keys;
    public MultiGet(List<byte[]> keys) {
      this.keys = keys;
    }
  }

}
