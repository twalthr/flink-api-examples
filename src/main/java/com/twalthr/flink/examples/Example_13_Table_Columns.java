package com.twalthr.flink.examples;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withColumns;

/** Example for dealing with many columns. */
public class Example_13_Table_Columns {

  public static void main(String[] args) {
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    // Leverage the power of code (loops, variables, etc.)
    // Simulate a large denormalized schema with 500 columns
    Schema.Builder schema = Schema.newBuilder();
    IntStream.rangeClosed(1, 250).forEach(i -> schema.column("customer_" + i, DataTypes.STRING()));
    IntStream.rangeClosed(1, 250)
        .forEach(i -> schema.column("attributes_" + i, DataTypes.STRING()));

    TableDescriptor descriptor =
        TableDescriptor.forConnector("datagen").schema(schema.build()).build();

    // Use withColumns to select or pass ranges of columns to functions
    env.from(descriptor)
        .select(
            $("customer_1"),
            call(CustomFunction.class, withColumns(range("attributes_1", "attributes_250"))))
        .as("customer", "combined_attributes")
        .execute()
        .print();
  }

  /** Var-arg UDF to illustrate how to deal with many columns. */
  public static class CustomFunction extends ScalarFunction {
    public String eval(String... args) {
      return Stream.of(args).map(a -> a.substring(1, 3)).sorted().collect(Collectors.joining(":"));
    }
  }
}
