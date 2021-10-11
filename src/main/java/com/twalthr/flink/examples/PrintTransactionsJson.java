package com.twalthr.flink.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.twalthr.flink.examples.Utils.getMapper;

/** Utilities to insert transactions dynamically into Kafka during a demo. */
public class PrintTransactionsJson {

  // bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092
  // --partitions=1 --replication-factor=1 &

  // bin/kafka-console-producer.sh --topic transactions --bootstrap-server localhost:9092

  // bin/kafka-console-consumer.sh --topic transactions --from-beginning --bootstrap-server
  // localhost:9092

  public static void main(String[] args) throws JsonProcessingException {
    ObjectMapper mapper = getMapper();
    for (Transaction transaction : ExampleData.TRANSACTIONS) {
      String s = mapper.writeValueAsString(transaction);
      System.out.println(s);
    }
  }
}
