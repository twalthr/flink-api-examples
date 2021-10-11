package com.twalthr.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Basic example of generating data and printing it. */
public class Example_01_DataStream_Motivation {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.fromElements(ExampleData.CUSTOMERS)
        .executeAndCollect()
        .forEachRemaining(System.out::println);
  }
}
