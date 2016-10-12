package kdb;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public final class Table implements Serializable {
  private static Logger log = LogManager.getLogger(Table.class);
  List<Column> cols;
  String name;

  public enum ColumnType {
    Int8, Int16, Int32, Int64, Float, Double,
    Varchar, Symbol, Blob, Timestamp, DateTime,
  }

  public static class Column implements Serializable {
    String name;
    ColumnType type;
    boolean iskey;

    public String getName() {
      return name;
    }

    public ColumnType getType() {
      return type;
    }

    public boolean iskey() {
      return iskey;
    }
  }

  public static class ColumnBuilder {
    private Column c;

    public ColumnBuilder() {
      c = new Column();
    }

    public Column column(){
      return c;
    }

    public void name(String name){
      c.name = name;
    }

    public void type(ColumnType type) {
      c.type = type;
    }

    public void key(boolean iskey) {
      c.iskey = iskey;
    }

    public void value(Object o) {
    }
  }

  public static class TableBuilder {
    Table t;

    public TableBuilder(String name) {
      t = new Table(name);
    }

    public static Table Table(String name, Consumer<TableBuilder> consumer) {
      TableBuilder builder = new TableBuilder(name);
      consumer.accept(builder);
      return builder.t;
    }

    public void column(Consumer<ColumnBuilder> consumer){
      ColumnBuilder builder = new ColumnBuilder();
      consumer.accept(builder);
      Column c = builder.column();
      t.addColumn(c);
    }
  }

  public static class Field {
    private String name;
    private String sval;
    private int ival;
    private long lval;
    private double dval;

  }

  public static class FieldBuilder {
    private Field f;

    public FieldBuilder() {
      f = new Field();
    }

    public Field field(){
      return f;
    }

    public void field(String name, int val) {
      f.name = name;
      f.ival = val;
    }

    public void field(String name, long val) {
      f.name = name;
      f.lval = val;
    }

    public void field(String name, double val) {
      f.name = name;
      f.dval = val;
    }

    public void field(String name, String val) {
      f.name = name;
      f.sval = val;
    }
  }

  public static class Row {
    List<Field> fields = new ArrayList<Field>();

    public void add(Field f) {
      fields.add(f);
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("row<");
      for(Field f : fields) {
      builder.append(f.name);
      builder.append("::");
      builder.append(f.ival);
      builder.append(" ");
    }
      builder.append(">");
      return builder.toString();
    }
  }

  public static class RowBuilder {
    Row r;

    public static Row Row(Consumer<RowBuilder> consumer) {
      RowBuilder builder = new RowBuilder();
      consumer.accept(builder);
      return builder.r;
    }

    public RowBuilder() {
      this.r = new Row();
    }

    public void field(Consumer<FieldBuilder> consumer) {
      FieldBuilder builder = new FieldBuilder();
      consumer.accept(builder);
      Field f = builder.field();
      r.add(f);
    }
  }

  public static Table Table(String name, Column... cols) {
    Table tbl = new Table(name);
    for(Column c : cols) {
      tbl.addColumn(c);
    }
    return tbl;
  }

  private Table(String name) {
    cols = new ArrayList<Column>();
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void addColumn(Column c) {
    cols.add(c);
  }

  public List<Column> getCols() {
    return cols;
  }

  public void insert(Row r) {
    System.out.println(r);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("table<");
    for(Column c : cols) {
      builder.append(c.getName());
      builder.append("::"+c.iskey());
      builder.append("::");
      builder.append(c.getType());
      builder.append(" ");
    }
    builder.append(">");
    return builder.toString();
  }

}
