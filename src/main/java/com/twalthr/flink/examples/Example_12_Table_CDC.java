package com.twalthr.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Use Flink's CDC connector ecosystem to connect to MySQL's binlog.
 *
 * <p>This example requires a running MySQL instance that can be started via {@link
 * StartMySqlContainer}.
 *
 * <p>{@link FillMySqlWithValues} can be executed to fill in new values and let the CDC show
 * real-time updates.
 *
 * <p>See also <a href="https://flink-packages.org/packages/cdc-connectors">CDC connectors</a>.
 */
public class Example_12_Table_CDC {

  static {
    // make console printing prettier
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  public static void main(String[] args) throws Exception {
    // setup environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    env.enableCheckpointing(200);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // create tables to connect to external systems

    // CDC for continuous updates
    tableEnv.createTable("TransactionsCDC", MySqlDescriptors.TRANSACTIONS_CDC);
    // JDBC one time scans and instant lookups
    tableEnv.createTable("CustomersJDBC", MySqlDescriptors.CUSTOMERS_JDBC);

    // read the CDC log
    tableEnv.executeSql("SELECT * FROM TransactionsCDC").print();

    // join the CDC log with real-time table lookups to the Customer table
    //    tableEnv
    //        .executeSql(
    //            "SELECT t_proctime, t_id, c_name "
    //                + "FROM TransactionsCDC "
    //                + "LEFT JOIN CustomersJDBC "
    //                + "FOR SYSTEM_TIME AS OF t_proctime ON t_customer_id = c_id")
    //        .print();

    // or switch to DataStream API for complex CDC processing
    //    DataStream<Row> cdcStream = tableEnv.toChangelogStream(tableEnv.from("TransactionsCDC"));
    //    cdcStream
    //        .filter(r -> r.getKind() != RowKind.DELETE)
    //        .executeAndCollect()
    //        .forEachRemaining(System.out::println);
  }
}
