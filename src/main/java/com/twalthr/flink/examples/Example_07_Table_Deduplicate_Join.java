package com.twalthr.flink.examples;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Use DataStream API connectors but deduplicate and join in SQL. */
public class Example_07_Table_Deduplicate_Join {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // switch to batch mode on demand
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    DataStream<Customer> customerStream = env.fromElements(ExampleData.CUSTOMERS);
    tableEnv.createTemporaryView("Customers", customerStream);

    // read transactions
    KafkaSource<Transaction> transactionSource =
        KafkaSource.<Transaction>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("transactions")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TransactionDeserializer())
            .setBounded(OffsetsInitializer.latest())
            .build();

    DataStream<Transaction> transactionStream =
        env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

    // seamlessly switch from DataStream to Table API
    tableEnv.createTemporaryView("Transactions", transactionStream);

    // use Flink SQL to do the heavy lifting
    tableEnv
        .executeSql(
            "SELECT c_name, CAST(t_amount AS DECIMAL(5, 2))\n"
                + "FROM Customers\n"
                + "JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id")
        .print();
  }
}
