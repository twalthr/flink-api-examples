package com.twalthr.flink.examples;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Inserts data into MySQL tables while {@link Example_12_Table_CDC} is running.
 *
 * <p>In theory, we could also use the MySQL client for this. But we leverage Flink also for this
 * task.
 */
public class FillMySqlWithValues {

  public static void main(String[] args) {
    // setup environment
    TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    tableEnv.getConfig().set(CoreOptions.DEFAULT_PARALLELISM, 1);

    // create tables for output
    tableEnv.createTable("TransactionsJDBC", MySqlDescriptors.TRANSACTIONS_JDBC);
    tableEnv.createTable("CustomersJDBC", MySqlDescriptors.CUSTOMERS_JDBC);

    // add data to MySQL
    // tableEnv.executeSql("INSERT INTO TransactionsJDBC VALUES (4, 1000, 2)");
    // tableEnv.executeSql("INSERT INTO TransactionsJDBC VALUES (5, 42, 3)");
    // tableEnv.executeSql("INSERT INTO CustomersJDBC VALUES (3, 'Kylie')");
    // tableEnv.executeSql("INSERT INTO TransactionsJDBC VALUES (6, 9999, 3)");
  }
}
