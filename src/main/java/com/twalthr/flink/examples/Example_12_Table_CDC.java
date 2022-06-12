package com.twalthr.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class Example_12_Table_CDC {

  static {
    // make console prettier
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    env.enableCheckpointing(200);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.createTable("TransactionsCDC", DatabaseDescriptors.TRANSACTIONS_CDC);
    tableEnv.createTable("CustomersJDBC", DatabaseDescriptors.CUSTOMERS_JDBC);

    // tableEnv.executeSql("SELECT * FROM TransactionsCDC").print();

    tableEnv
        .executeSql(
            "SELECT * "
                + "FROM TransactionsCDC "
                + "LEFT JOIN CustomersJDBC FOR SYSTEM_TIME AS OF t_proctime ON t_customer_id = c_id")
        .print();
  }
}
