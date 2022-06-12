package com.twalthr.flink.examples;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FillMySqlWithStatements {

  public static void main(String[] args) {
    TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    tableEnv.getConfig().set(CoreOptions.DEFAULT_PARALLELISM, 1);

    tableEnv.createTable("TransactionsJDBC", DatabaseDescriptors.TRANSACTIONS_JDBC);
    tableEnv.createTable("CustomersJDBC", DatabaseDescriptors.CUSTOMERS_JDBC);

    tableEnv.executeSql("INSERT INTO TransactionsJDBC VALUES (4, 1000, 2)");
    // tableEnv.executeSql("INSERT INTO TransactionsJDBC VALUES (5, 42, 3)");
    // tableEnv.executeSql("INSERT INTO CustomersJDBC VALUES (3, 'Kylie')");
    // tableEnv.executeSql("INSERT INTO TransactionsJDBC VALUES (6, 9999, 3)");
  }
}
