package settlements.obligations

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import settlements.obligations.topology.ApplyConfirmations
import settlements.obligations.topology.CreateOrUpdateObligationState
import settlements.obligations.topology.PersistObligations
import settlements.obligations.topology.Topics
import java.util.*

class ObligationsConsumer {

  fun run() {
    val streamsConfiguration = Properties()
    streamsConfiguration.putAll(mapOf(
        StreamsConfig.APPLICATION_ID_CONFIG to "obligation-source",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092",
        "schema.registry.url" to "http://localhost:8081",
        StreamsConfig.STATE_DIR_CONFIG to "data",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to SpecificAvroSerde::class.qualifiedName
    ))
    val streams = KafkaStreams(topology(SpecificAvroSerde<SpecificRecord>(), "http://localhost:8081"), streamsConfiguration)
    streams.start()
    Runtime.getRuntime().addShutdownHook(Thread(streams::close))
  }

  companion object {
    fun topology(avroSerde: Serde<SpecificRecord>, schemaUrl: String): Topology {
      val topology = Topology()
      avroSerde.configure(mapOf(
          "specific.avro.reader" to true,
          "schema.registry.url" to schemaUrl
      ), false)
      topology.addStateStore(ObligationStateStore(avroSerde))

      PersistObligations(topology)
      CreateOrUpdateObligationState(topology)
      ApplyConfirmations(topology)

      topology.addSink("ObligationsState", Topics.ObligationState, ApplyConfirmations.name, CreateOrUpdateObligationState.name)

      return topology
    }
  }
}

fun main(args: Array<String>) {
  ObligationsConsumer().run()
}