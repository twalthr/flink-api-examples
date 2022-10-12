package com.twalthr.flink.examples;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class MySqlDescriptors {

  public static final TableDescriptor TRANSACTIONS_CDC =
      TableDescriptor.forConnector("mysql-cdc")
          .schema(
              Schema.newBuilder()
                  .column("t_id", DataTypes.BIGINT().notNull())
                  .column("t_amount", DataTypes.INT().notNull())
                  .column("t_customer_id", DataTypes.BIGINT().notNull())
                  .columnByExpression("t_proctime", "PROCTIME()")
                  .primaryKey("t_id")
                  .build())
          .option("hostname", "localhost")
          .option("port", StartMySqlContainer.getPort())
          .option("username", "reader")
          .option("password", "password")
          .option("database-name", "example_db")
          .option("table-name", "Transactions")
          .option("server-id", "5800-5900")
          .option("heartbeat.interval", "1s")
          .build();

  public static final TableDescriptor TRANSACTIONS_JDBC =
      TableDescriptor.forConnector("jdbc")
          .schema(
              Schema.newBuilder()
                  .column("t_id", DataTypes.BIGINT().notNull())
                  .column("t_amount", DataTypes.INT().notNull())
                  .column("t_customer_id", DataTypes.BIGINT().notNull())
                  .primaryKey("t_id")
                  .build())
          .option("url", "jdbc:mysql://localhost:" + StartMySqlContainer.getPort() + "/example_db")
          .option("table-name", "Transactions")
          .option("username", "inserter")
          .option("password", "password")
          .build();

  public static final TableDescriptor CUSTOMERS_JDBC =
      TableDescriptor.forConnector("jdbc")
          .schema(
              Schema.newBuilder()
                  .column("c_id", DataTypes.BIGINT().notNull())
                  .column("c_name", DataTypes.STRING().notNull())
                  .primaryKey("c_id")
                  .build())
          .option("url", "jdbc:mysql://localhost:" + StartMySqlContainer.getPort() + "/example_db")
          .option("table-name", "Customers")
          .option("username", "inserter")
          .option("password", "password")
          .build();

  private MySqlDescriptors() {}
}
