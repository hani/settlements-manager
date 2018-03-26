package io.confluent.kafka.streams.serdes.avro

import io.confluent.kafka.serializers.KafkaAvroDeserializer

class InMemoryKafkaAvroDeserializer : KafkaAvroDeserializer(InMemorySpecificAvroSerde.SchemaRegistryClient)