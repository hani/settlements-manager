package settlements.obligations

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import settlements.Obligation
import settlements.ObligationState

/**
 * @author Hani Suleiman
 */
object SettlementsSerdes {
  
  fun obligation(client: SchemaRegistryClient): Serde<Obligation> {
    val deserializer = KafkaAvroDeserializer(client)
    deserializer.configure(mapOf(
        "specific.avro.reader" to true,
        "schema.registry.url" to "http://dummy"
    ), false)
    return Serdes.serdeFrom<Any>(KafkaAvroSerializer(client), deserializer) as Serde<Obligation>
  }
  
  fun obligationState(client: SchemaRegistryClient): Serde<ObligationState> {
    val deserializer = KafkaAvroDeserializer(client)
    deserializer.configure(mapOf(
        "specific.avro.reader" to true,
        "schema.registry.url" to "http://dummy"
    ), false)
    return Serdes.serdeFrom<Any>(KafkaAvroSerializer(client), deserializer) as Serde<ObligationState>
  }
}