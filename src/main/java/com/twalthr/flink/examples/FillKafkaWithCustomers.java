package com.twalthr.flink.examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;
import java.time.LocalDate;

public class FillKafkaWithCustomers {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    DataStream<Row> customerStream =
        env.fromElements(
                Row.ofKind(
                    RowKind.INSERT,
                    Instant.parse("2021-10-01T12:00:00.000Z"),
                    12L,
                    "Alice",
                    LocalDate.of(1984, 3, 12)),
                Row.ofKind(
                    RowKind.INSERT,
                    Instant.parse("2021-10-01T12:00:00.000Z"),
                    32L,
                    "Bob",
                    LocalDate.of(1990, 10, 14)),
                Row.ofKind(
                    RowKind.INSERT,
                    Instant.parse("2021-10-01T12:00:00.000Z"),
                    7L,
                    "Kyle",
                    LocalDate.of(1979, 2, 23)),
                Row.ofKind(
                    RowKind.UPDATE_AFTER,
                    Instant.parse("2021-10-02T09:00:00.000Z"),
                    7L,
                    "Kylie",
                    LocalDate.of(1984, 3, 12)),
                Row.ofKind(
                    RowKind.UPDATE_AFTER,
                    Instant.parse("2021-10-10T08:00:00.000Z"),
                    12L,
                    "Aliceson",
                    LocalDate.of(1984, 3, 12)),
                Row.ofKind(
                    RowKind.INSERT,
                    Instant.parse("2021-10-20T12:00:00.000Z"),
                    77L,
                    "Robert",
                    LocalDate.of(2002, 7, 20)))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"c_update_time", "c_id", "c_name", "c_birthday"},
                    Types.INSTANT,
                    Types.LONG,
                    Types.STRING,
                    Types.LOCAL_DATE));

    tableEnv
        .fromChangelogStream(customerStream)
        .executeInsert(KafkaDescriptors.CUSTOMERS_DESCRIPTOR);
  }
}
