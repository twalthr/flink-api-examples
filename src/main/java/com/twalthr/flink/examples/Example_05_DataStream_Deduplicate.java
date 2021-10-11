package com.twalthr.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/** Use Flink's state to perform efficient record deduplication. */
public class Example_05_DataStream_Deduplicate {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // set up a Kafka source
    KafkaSource<Transaction> transactionSource =
        KafkaSource.<Transaction>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("transactions")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TransactionDeserializer())
            .build();

    DataStream<Transaction> transactionStream =
        env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

    transactionStream
        .keyBy(t -> t.t_id)
        .process(
            new KeyedProcessFunction<Long, Transaction, Transaction>() {

              // use Flink's managed keyed state
              ValueState<Transaction> seen;

              @Override
              public void open(Configuration parameters) {
                seen =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("seen", Transaction.class));
              }

              @Override
              public void processElement(
                  Transaction transaction,
                  KeyedProcessFunction<Long, Transaction, Transaction>.Context context,
                  Collector<Transaction> out)
                  throws Exception {
                if (seen.value() == null) {
                  seen.update(transaction);
                  // use timers to clean up state
                  context
                      .timerService()
                      .registerProcessingTimeTimer(
                          context.timerService().currentProcessingTime()
                              + Duration.ofHours(1).toMillis());
                  out.collect(transaction);
                }
              }

              @Override
              public void onTimer(
                  long timestamp,
                  KeyedProcessFunction<Long, Transaction, Transaction>.OnTimerContext ctx,
                  Collector<Transaction> out) {
                seen.clear();
              }
            })
        .executeAndCollect()
        .forEachRemaining(System.out::println);
  }
}
