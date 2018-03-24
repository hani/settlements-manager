package io.confluent.kafka.streams.serdes.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;

/**
 * This class has to be in this package because the constructor which takes in an explicit client
 * is package protected, and for in memory testing, we need to use that constructor to pass in the mock schema registry client
 */
public class InMemorySpecificAvroSerde<T extends SpecificRecord> extends SpecificAvroSerde<T>{
  
  //singleton here so we can reach in add schemas to it in tests
  public static final SchemaRegistryClient SchemaRegistryClient = new MockSchemaRegistryClient();
  
  public InMemorySpecificAvroSerde() {
    super(SchemaRegistryClient);
  }
}
