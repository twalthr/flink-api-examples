package com.twalthr.flink.examples;

import java.time.LocalDate;
import java.util.Objects;

public class Customer {
  public long c_id;
  public String c_name;
  public LocalDate c_birthday;

  public Customer() {}

  public Customer(long c_id, String c_name, LocalDate c_birthday) {
    this.c_id = c_id;
    this.c_name = c_name;
    this.c_birthday = c_birthday;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Customer customer = (Customer) o;
    return c_id == customer.c_id
        && c_name.equals(customer.c_name)
        && c_birthday.equals(customer.c_birthday);
  }

  @Override
  public int hashCode() {
    return Objects.hash(c_id, c_name, c_birthday);
  }

  @Override
  public String toString() {
    return "Customer(" + "c_id=" + c_id + ", c_name=" + c_name + ", c_birthday=" + c_birthday + ')';
  }
}
