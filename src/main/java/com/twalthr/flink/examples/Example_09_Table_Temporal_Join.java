package com.twalthr.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

import java.time.ZoneId;

/** Perform the materialized view maintenance smarter by using time-versioned joins. */
public class Example_09_Table_Temporal_Join {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // due to little data
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    TableConfig config = tableEnv.getConfig();
    config.setLocalTimeZone(ZoneId.of("UTC"));

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

    tableEnv.createTemporaryView(
        "Transactions",
        transactionStream,
        Schema.newBuilder()
            .columnByExpression("t_rowtime", "CAST(t_time AS TIMESTAMP_LTZ(3))")
            .watermark("t_rowtime", "t_rowtime - INTERVAL '10' SECONDS")
            .build());

    Table deduplicateTransactions =
        tableEnv.sqlQuery(
            "SELECT t_id, t_rowtime, t_customer_id, t_amount\n"
                + "FROM (\n"
                + "   SELECT *,\n"
                + "      ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY t_rowtime) AS row_num\n"
                + "   FROM Transactions)\n"
                + "WHERE row_num = 1");
    tableEnv.createTemporaryView("DeduplicateTransactions", deduplicateTransactions);

    // use a customer changelog with timestamps
    DataStream<Row> customerStream =
        env.fromElements(ExampleData.CUSTOMERS_WITH_TEMPORAL_UPDATES)
            .returns(
                Types.ROW_NAMED(
                    new String[] {"c_update_time", "c_id", "c_name", "c_birthday"},
                    Types.INSTANT,
                    Types.LONG,
                    Types.STRING,
                    Types.LOCAL_DATE));

    // make it a temporal view
    tableEnv.createTemporaryView(
        "Customers",
        tableEnv.fromChangelogStream(
            customerStream,
            Schema.newBuilder()
                .columnByExpression("c_rowtime", "CAST(c_update_time AS TIMESTAMP_LTZ(3))")
                .primaryKey("c_id")
                .watermark("c_rowtime", "c_rowtime - INTERVAL '10' SECONDS")
                .build(),
            ChangelogMode.upsert()));

    tableEnv
        .executeSql(
            "SELECT t_rowtime, c_rowtime, t_id, c_name, t_amount\n"
                + "FROM DeduplicateTransactions\n"
                + "LEFT JOIN Customers FOR SYSTEM_TIME AS OF t_rowtime\n"
                + "   ON c_id = t_customer_id")
        .print();
  }
}
