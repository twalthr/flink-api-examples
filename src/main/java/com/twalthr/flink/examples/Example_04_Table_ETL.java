package com.twalthr.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.TimePointUnit;

import java.time.LocalDate;

import static org.apache.flink.table.api.Expressions.*;

/** Use built-in functions to perform streaming ETL i.e. convert records into JSON. */
public class Example_04_Table_ETL {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment env = StreamTableEnvironment.create(streamEnv);

    final Table t =
        env.fromValues(
                row(12L, "Alice", LocalDate.of(1984, 3, 12)),
                row(32L, "Bob", LocalDate.of(1990, 10, 14)),
                row(7L, "Kyle", LocalDate.of(1979, 2, 23)))
            .as("c_id", "c_name", "c_birthday")
            .select(
                jsonObject(
                    JsonOnNull.NULL,
                    "name",
                    $("c_name"),
                    "age",
                    timestampDiff(TimePointUnit.YEAR, $("c_birthday"), currentDate())));

    env.toChangelogStream(t).print();

    streamEnv.execute();
  }
}
