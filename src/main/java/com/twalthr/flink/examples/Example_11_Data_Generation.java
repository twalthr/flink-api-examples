package com.twalthr.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Example for generating demo data and use it in both Table API and DataStream API. */
public class Example_11_Data_Generation {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // -- Create a table from a DataGen source

    // By default, the source is unbounded.

    Table table =
        tableEnv.from(
            TableDescriptor.forConnector("datagen")
                .schema(
                    Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .build())
                .option("rows-per-second", "10")
                .option("fields.id.min", "100")
                .option("fields.id.max", "999")
                .option("fields.name.length", "32")
                .build());

    // -- Print locally while staying in Table API

    table.execute().print();

    // -- Switch to DataStream API using Row

    // Note that Row classes are not always easy to handle because DataStream APIs
    // type extraction cannot extract serializers easily via reflection.

    // DataStream<Row> rowDataStream = tableEnv.toDataStream(table);
    // rowDataStream.executeAndCollect().forEachRemaining(System.out::println);

    // -- Switch to DataStream API using regular Java objects (i.e. POJOs)

    // Note that the POJO is analyzed using reflection and the type system of the Table API.

    // Make sure that the table's schema (names and data types) matches with the POJO.
    // The conversion classes must be supported e.g. TIMESTAMP_LTZ -> java.time.Instant is supported
    // but DATE -> java.util.Date is not.

    // The mapping happens based on name and implicit casts are inserted where appropriate.
    // If the automatic analyzing does not work, use toDataStream(Table, DataType).

    // DataStream<CustomerEvent> pojoDataStream = tableEnv.toDataStream(table, CustomerEvent.class);
    // pojoDataStream.executeAndCollect().forEachRemaining(System.out::println);
  }

  @SuppressWarnings("unused")
  public static class CustomerEvent {
    private final Long id;
    private final String name;

    public CustomerEvent(Long id, String name) {
      this.id = id;
      this.name = name;
    }

    public Long getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return "CustomerEvent{" + "id=" + id + ", name='" + name + '\'' + '}';
    }
  }
}
