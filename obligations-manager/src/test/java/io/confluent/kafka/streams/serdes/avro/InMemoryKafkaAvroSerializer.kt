package io.confluent.kafka.streams.serdes.avro

import io.confluent.kafka.serializers.KafkaAvroSerializer

class InMemoryKafkaAvroSerializer : KafkaAvroSerializer(InMemorySpecificAvroSerde.SchemaRegistryClient)