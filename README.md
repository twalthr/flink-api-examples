# Flink API Examples for DataStream API and Table API

The Table API is not a new kid on the block. But the community has worked hard on reshaping its future. Today, it is one
of the core abstractions in Flink next to the DataStream API. The Table API can deal with bounded and unbounded streams
in a unified and highly optimized ecosystem inspired by databases and SQL. Various connectors and catalogs integrate
with the outside world.

But this doesn't mean that the DataStream API will become obsolete any time soon. This repository demos what Table API
is capable of today. We present how the API solves different scenarios:

- as a batch processor,
- a changelog processor,
- a change data capture (CDC) hub,
- or a streaming ETL tool

with many built-in functions and operators for deduplicating, joining, and aggregating data.

It shows hybrid pipelines in which both APIs interact in symbiosis and contribute their unique strengths.

# How to Use This Repository

1. Import this repository into your IDE (preferably IntelliJ IDEA). Select the `pom.xml` file during import to treat it
   as a Maven project. The project uses the latest Flink 1.15.

2. All examples are runnable from the IDE. You simply need to execute the `main()` method of every example class.

3. In order to make the examples run within IntelliJ IDEA, it is necessary to tick
   the `Add dependencies with "provided" scope to classpath` option in the run configuration under `Modify options`.

4. For the Apache Kafka examples, download and unzip [Apache Kafka](https://kafka.apache.org/downloads). Start up Kafka
   and Zookeeper:

   ```
   ./bin/zookeeper-server-start.sh config/zookeeper.properties &
   
   ./bin/kafka-server-start.sh config/server.properties &
   ```

   Run `FillKafkaWithCustomers` and `FillKafkaWithTransactions` to create and fill the Kafka topics with Flink.

5. For the MySQL CDC example, run `StartMySqlContainer` with an available Docker setup to set up a dummy database
   instance. `FillMySqlWithValues` provides a Flink job to update the database tables while the CDC example is running.


