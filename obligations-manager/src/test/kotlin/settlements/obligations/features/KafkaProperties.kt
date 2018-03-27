package settlements.obligations.features

import io.confluent.kafka.streams.serdes.avro.InMemoryKafkaAvroDeserializer
import io.confluent.kafka.streams.serdes.avro.InMemoryKafkaAvroSerializer
import io.confluent.kafka.streams.serdes.avro.InMemorySpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import java.util.*

object KafkaProperties {

  fun streams(appId: String, schemaUrl: String, bootstrapServers: String): Properties {
    val properties = Properties()

    properties.putAll(mapOf(
        "bootstrap.servers" to bootstrapServers,
        "application.id" to appId,
        "auto.offset.reset" to "earliest",
        "schema.registry.url" to schemaUrl,
        StreamsConfig.STATE_DIR_CONFIG to "data",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to InMemorySpecificAvroSerde::class.qualifiedName
    ))
    return properties
  }

  fun producer(bootstrapServers: String): Properties {
    val properties = Properties()
    properties.putAll(mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.RETRIES_CONFIG to 0,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        "schema.registry.url" to "http://ignored_for_inmemory",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to InMemoryKafkaAvroSerializer::class.java
    )
    )
    return properties
  }

  fun consumer(bootstrapServers: String): Properties {
    val properties = Properties()
    properties.putAll(mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to "cucumber-consumer",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to InMemoryKafkaAvroDeserializer::class.java,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
        "specific.avro.reader" to true,
        "schema.registry.url" to "http://ignored_for_inmemory"
    )
    )
    return properties
  }
}