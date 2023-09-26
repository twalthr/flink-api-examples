package com.twalthr.flink.examples;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/** Hello world example. */
public class Example_00_Table_Hello_World {

  public static void main(String[] args) {
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    // Fully programmatic
    env.fromValues(1).execute().print();

    // Flink SQL is included
    env.sqlQuery("SELECT 1").execute().print();

    // The API feels like SQL (built-in functions, data types, ...)
    Table table = env.fromValues("1").as("c").select($("c").cast(DataTypes.STRING()));

    // Everything is centered around Table objects, conceptually SQL views (=virtual tables)
    env.createTemporaryView("InputTable", table);

    // Catalogs and metadata management are the foundation
    env.from("InputTable").insertInto(TableDescriptor.forConnector("blackhole").build()).execute();

    // Let's get started with unbounded data...
    env.from(
            TableDescriptor.forConnector("datagen")
                .schema(
                    Schema.newBuilder()
                        .column("uid", DataTypes.BIGINT())
                        .column("s", DataTypes.STRING())
                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                        .build())
                .build())
        .execute()
        .print();
  }
}
