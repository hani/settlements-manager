package settlements.obligations

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import settlements.Obligation
import settlements.ObligationState

@Suppress("UNCHECKED_CAST")
object SettlementsSerdes {

  private val config = mapOf(
      "specific.avro.reader" to true,
      "schema.registry.url" to "http://ignored"
  )

  fun obligation(client: SchemaRegistryClient): Serde<Obligation> {
    val deserializer = KafkaAvroDeserializer(client)
    deserializer.configure(config, false)
    return Serdes.serdeFrom<Any>(KafkaAvroSerializer(client), deserializer) as Serde<Obligation>
  }
  
  fun obligationState(client: SchemaRegistryClient): Serde<ObligationState> {
    val deserializer = KafkaAvroDeserializer(client)
    deserializer.configure(config, false)
    return Serdes.serdeFrom<Any>(KafkaAvroSerializer(client), deserializer) as Serde<ObligationState>
  }
}