package com.twalthr.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

/** Maintain a materialized view. */
public class Example_08_Table_Updating_Join {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // read transactions
    KafkaSource<Transaction> transactionSource =
        KafkaSource.<Transaction>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("transactions")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TransactionDeserializer())
            .build();

    DataStream<Transaction> transactionStream =
        env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

    tableEnv.createTemporaryView("Transactions", transactionStream);

    // use a customer changelog
    DataStream<Row> customerStream =
        env.fromElements(ExampleData.CUSTOMERS_WITH_UPDATES)
            .returns(
                Types.ROW_NAMED(
                    new String[] {"c_id", "c_name", "c_birthday"},
                    Types.LONG,
                    Types.STRING,
                    Types.LOCAL_DATE));

    // make it an updating view
    tableEnv.createTemporaryView(
        "Customers",
        tableEnv.fromChangelogStream(
            customerStream,
            Schema.newBuilder().primaryKey("c_id").build(),
            ChangelogMode.upsert()));

    // query the changelog backed view
    // and thus perform materialized view maintenance
    tableEnv
        .executeSql(
            "SELECT c_name, t_amount\n"
                + "FROM Customers\n"
                + "LEFT JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id")
        .print();
  }
}
