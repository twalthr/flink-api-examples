package com.twalthr.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/** Use Flink's state to perform efficient record joining based on business requirements. */
public class Example_06_DataStream_Join {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // switch to batch mode on demand
    // env.setRuntimeMode(RuntimeExecutionMode.BATCH);

    // read transactions
    KafkaSource<Transaction> transactionSource =
        KafkaSource.<Transaction>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("transactions")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new TransactionDeserializer())
            // .setBounded(OffsetsInitializer.latest())
            .build();

    DataStream<Transaction> transactionStream =
        env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

    // deduplicate transactions
    DataStream<Transaction> deduplicateStream =
        transactionStream
            .keyBy(t -> t.t_id)
            .process(
                new KeyedProcessFunction<Long, Transaction, Transaction>() {

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
                });

    DataStream<Customer> customerStream = env.fromElements(ExampleData.CUSTOMERS);

    // join transactions and customers
    customerStream
        .connect(deduplicateStream)
        .keyBy(c -> c.c_id, t -> t.t_customer_id)
        .process(
            new KeyedCoProcessFunction<Long, Customer, Transaction, String>() {

              ValueState<Customer> customer;

              ListState<Transaction> transactions;

              @Override
              public void open(Configuration parameters) {
                customer =
                    getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("customer", Customer.class));
                transactions =
                    getRuntimeContext()
                        .getListState(new ListStateDescriptor<>("transactions", Transaction.class));
              }

              @Override
              public void processElement1(
                  Customer c,
                  KeyedCoProcessFunction<Long, Customer, Transaction, String>.Context ctx,
                  Collector<String> out)
                  throws Exception {
                customer.update(c);
                Iterator<Transaction> txs = transactions.get().iterator();
                // buffer transactions if the other stream is not ready yet
                if (!txs.hasNext()) {
                  performJoin(out, c, txs);
                }
              }

              @Override
              public void processElement2(
                  Transaction tx,
                  KeyedCoProcessFunction<Long, Customer, Transaction, String>.Context ctx,
                  Collector<String> out)
                  throws Exception {
                transactions.add(tx);
                Customer c = customer.value();
                // buffer customer if the other stream is not ready yet
                if (customer.value() != null) {
                  performJoin(out, c, transactions.get().iterator());
                }
              }

              private void performJoin(
                  Collector<String> out, Customer c, Iterator<Transaction> txs) {
                txs.forEachRemaining(tx -> out.collect(c.c_name + ": " + tx.t_amount));
                transactions.clear();
              }
            })
        .executeAndCollect()
        .forEachRemaining(System.out::println);
  }
}
