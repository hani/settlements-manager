package io.confluent.kafka.streams.serdes.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;

public class InMemorySpecificAvroSerde<T extends SpecificRecord> extends SpecificAvroSerde<T>{
  
  //singleton here so we can add schemas to it in tests
  public static final SchemaRegistryClient SchemaRegistryClient = new MockSchemaRegistryClient();
  
  public InMemorySpecificAvroSerde() {
    super(SchemaRegistryClient);
  }
}
