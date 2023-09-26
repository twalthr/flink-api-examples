package com.twalthr.flink.examples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;

public class FillKafkaWithOrders {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    DataStream<Row> orderStream =
        env.fromElements(
                Row.of(Instant.parse("2021-10-01T12:00:00.000Z"), 46L, "CHECKOUT"),
                Row.of(Instant.parse("2021-10-01T12:00:30.000Z"), 12L, "CHECKOUT"),
                Row.of(Instant.parse("2021-10-01T12:00:31.000Z"), 20L, "CHECKOUT"),
                Row.of(Instant.parse("2021-10-01T12:01:00.000Z"), 46L, "PAID"),
                Row.of(Instant.parse("2021-10-01T12:01:30.000Z"), 46L, "CONFIRMED"),
                Row.of(Instant.parse("2021-10-01T12:01:40.000Z"), 97L, "CONFIRMED"),
                Row.of(Instant.parse("2021-10-01T12:01:41.000Z"), 97L, "CANCELLED"))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"o_rowtime", "o_id", "o_status"},
                    Types.INSTANT,
                    Types.LONG,
                    Types.STRING));

    tableEnv.fromDataStream(orderStream).executeInsert(KafkaDescriptors.ORDERS_DESCRIPTOR);
  }
}
