package com.twalthr.flink.examples;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class KafkaDescriptors {

  public static TableDescriptor CUSTOMERS_DESCRIPTOR =
      TableDescriptor.forConnector("upsert-kafka")
          .schema(
              Schema.newBuilder()
                  .column("c_rowtime", DataTypes.TIMESTAMP_LTZ(3))
                  .column("c_id", DataTypes.BIGINT().notNull())
                  .column("c_name", DataTypes.STRING())
                  .column("c_birthday", DataTypes.DATE())
                  .primaryKey("c_id")
                  .watermark("c_rowtime", "c_rowtime - INTERVAL '10' SECONDS")
                  .build())
          .option("key.format", "json")
          .option("value.format", "json")
          .option("topic", "customers")
          .option("properties.bootstrap.servers", "localhost:9092")
          .build();

  public static TableDescriptor TRANSACTIONS_DESCRIPTOR =
      TableDescriptor.forConnector("kafka")
          .schema(
              Schema.newBuilder()
                  .column("t_time", DataTypes.TIMESTAMP_LTZ(3))
                  .column("t_id", DataTypes.BIGINT().notNull())
                  .column("t_customer_id", DataTypes.BIGINT().notNull())
                  .column("t_amount", DataTypes.DECIMAL(5, 2))
                  .watermark("t_time", "t_time - INTERVAL '10' SECONDS")
                  .build())
          .format("json")
          .option("json.timestamp-format.standard", "ISO-8601")
          .option("topic", "transactions")
          .option("scan.startup.mode", "earliest-offset")
          .option("properties.bootstrap.servers", "localhost:9092")
          .build();

  public static TableDescriptor ORDERS_DESCRIPTOR =
      TableDescriptor.forConnector("kafka")
          .schema(
              Schema.newBuilder()
                  .column("o_rowtime", DataTypes.TIMESTAMP_LTZ(3))
                  .column("o_id", DataTypes.BIGINT().notNull())
                  .column("o_status", DataTypes.STRING())
                  .watermark("o_rowtime", "o_rowtime - INTERVAL '1' SECONDS")
                  .build())
          .option("format", "json")
          .option("topic", "orders")
          .option("scan.startup.mode", "earliest-offset")
          .option("properties.bootstrap.servers", "localhost:9092")
          .build();

  private KafkaDescriptors() {}
}
