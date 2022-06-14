package com.twalthr.flink.examples;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FillKafkaWithTransactions {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    DataStream<Transaction> transactionStream = env.fromElements(ExampleData.TRANSACTIONS);

    tableEnv
        .fromDataStream(transactionStream)
        .executeInsert(KafkaDescriptors.TRANSACTIONS_DESCRIPTOR);
  }
}
