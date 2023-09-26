package com.twalthr.flink.examples;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.LocalDate;
import java.util.Iterator;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Example for dealing with Plain-old Java Objects (POJOs) instead of rows. */
public class Example_14_Table_Objects {

  public static void main(String[] args) {
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    Table customerTopic = env.from(KafkaDescriptors.CUSTOMERS_DESCRIPTOR);

    // UDFs allow to create a SQL structured type
    Table customers =
        customerTopic.select(
            $("c_rowtime").as("rowtime"),
            call(CustomerOf.class, $("c_id"), $("c_name"), $("c_birthday")).as("customer"));

    // Structured types are natively supported by the engine and can be queried e.g. for filtering
    Iterator<Row> filteredCustomers =
        customers.filter($("customer").get("c_name").like("A%")).execute().collect();

    filteredCustomers.forEachRemaining(
        customerEvent -> {
          RowKind change = customerEvent.getKind();
          // Customer object is preserved and present in the output
          Customer customer = customerEvent.getFieldAs("customer");

          System.out.println(change.name());
          System.out.println(customer);
          System.out.println();
        });
  }

  /** Constructor function for {@link Customer}. */
  public static class CustomerOf extends ScalarFunction {
    public Customer eval(long c_id, String c_name, LocalDate c_birthday) {
      return new Customer(c_id, c_name, c_birthday);
    }
  }
}
