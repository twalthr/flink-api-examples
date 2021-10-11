package com.twalthr.flink.examples;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class TransactionDeserializer implements DeserializationSchema<Transaction> {

  @Override
  public Transaction deserialize(byte[] bytes) throws IOException {
    return Utils.getMapper().readValue(bytes, Transaction.class);
  }

  @Override
  public boolean isEndOfStream(Transaction transaction) {
    return false;
  }

  @Override
  public TypeInformation<Transaction> getProducedType() {
    return TypeInformation.of(Transaction.class);
  }
}
