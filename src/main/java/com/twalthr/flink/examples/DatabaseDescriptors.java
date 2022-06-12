package com.twalthr.flink.examples;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class DatabaseDescriptors {

  public static final String MYSQL_PORT = "50490";

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
          .option("port", MYSQL_PORT)
          .option("username", "reader")
          .option("password", "password")
          .option("database-name", "example_db")
          .option("table-name", "Transactions")
          .option("server-id", "5800-5900")
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
          .option("url", "jdbc:mysql://localhost:" + MYSQL_PORT + "/example_db")
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
          .option("url", "jdbc:mysql://localhost:" + MYSQL_PORT + "/example_db")
          .option("table-name", "Customers")
          .option("username", "inserter")
          .option("password", "password")
          .build();
}
