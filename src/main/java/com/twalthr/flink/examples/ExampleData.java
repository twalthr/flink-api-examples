package com.twalthr.flink.examples;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

public class ExampleData {

  public static final Transaction[] TRANSACTIONS =
      new Transaction[] {
        new Transaction(
            Instant.parse("2021-10-08T12:33:12.000Z"), 1L, 12L, new BigDecimal("325.12")),
        new Transaction(Instant.parse("2021-10-10T08:00:00.000Z"), 2L, 7L, new BigDecimal("13.99")),
        new Transaction(Instant.parse("2021-10-10T08:00:00.000Z"), 2L, 7L, new BigDecimal("13.99")),
        new Transaction(
            Instant.parse("2021-10-14T17:04:00.000Z"), 3L, 12L, new BigDecimal("52.48")),
        new Transaction(
            Instant.parse("2021-10-14T17:06:00.000Z"), 4L, 32L, new BigDecimal("26.11")),
        new Transaction(Instant.parse("2021-10-14T18:23:00.000Z"), 5L, 32L, new BigDecimal("22.03"))
      };

  public static final Customer[] CUSTOMERS =
      new Customer[] {
        new Customer(12L, "Alice", LocalDate.of(1984, 3, 12)),
        new Customer(32L, "Bob", LocalDate.of(1990, 10, 14)),
        new Customer(7L, "Kyle", LocalDate.of(1979, 2, 23))
      };

  public static final Row[] CUSTOMERS_WITH_UPDATES =
      new Row[] {
        Row.ofKind(RowKind.INSERT, 12L, "Alice", LocalDate.of(1984, 3, 12)),
        Row.ofKind(RowKind.INSERT, 32L, "Bob", LocalDate.of(1990, 10, 14)),
        Row.ofKind(RowKind.INSERT, 7L, "Kyle", LocalDate.of(1979, 2, 23)),
        Row.ofKind(RowKind.UPDATE_AFTER, 7L, "Kylie", LocalDate.of(1984, 3, 12)),
        Row.ofKind(RowKind.UPDATE_AFTER, 12L, "Aliceson", LocalDate.of(1984, 3, 12)),
        Row.ofKind(RowKind.INSERT, 77L, "Robert", LocalDate.of(1984, 3, 12))
      };

  public static final Row[] CUSTOMERS_WITH_TEMPORAL_UPDATES =
      new Row[] {
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
            LocalDate.of(2002, 7, 20))
      };
}
