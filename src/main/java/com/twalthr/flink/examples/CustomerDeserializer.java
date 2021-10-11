package com.twalthr.flink.examples;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class CustomerDeserializer implements DeserializationSchema<Customer> {

  @Override
  public Customer deserialize(byte[] bytes) throws IOException {
    return Utils.getMapper().readValue(bytes, Customer.class);
  }

  @Override
  public boolean isEndOfStream(Customer transaction) {
    return false;
  }

  @Override
  public TypeInformation<Customer> getProducedType() {
    return TypeInformation.of(Customer.class);
  }
}
