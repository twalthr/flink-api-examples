package com.twalthr.flink.examples;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;

/** Table API end-to-end example with time-versioned joins. */
public class Example_10_Table_Temporal_Join_Pure {

  public static void main(String[] args) {
    TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    TableConfig config = tableEnv.getConfig();
    config.getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1); // due to little data
    config.setLocalTimeZone(ZoneId.of("UTC"));

    // use descriptor API to use dedicated table connectors
    tableEnv.createTemporaryTable("Customers", KafkaDescriptors.CUSTOMERS_DESCRIPTOR);
    tableEnv.createTemporaryTable("Transactions", KafkaDescriptors.TRANSACTIONS_DESCRIPTOR);

    Table deduplicateTransactions =
        tableEnv.sqlQuery(
            "SELECT t_id, t_time, t_customer_id, t_amount\n"
                + "FROM (\n"
                + "   SELECT *,\n"
                + "      ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY t_time) AS row_num\n"
                + "   FROM Transactions)\n"
                + "WHERE row_num = 1");
    tableEnv.createTemporaryView("DeduplicateTransactions", deduplicateTransactions);

    tableEnv
        .executeSql(
            "SELECT t_time, c_rowtime, t_id, c_name, t_amount\n"
                + "FROM DeduplicateTransactions\n"
                + "LEFT JOIN Customers FOR SYSTEM_TIME AS OF t_time ON c_id = t_customer_id")
        .print();
  }
}
