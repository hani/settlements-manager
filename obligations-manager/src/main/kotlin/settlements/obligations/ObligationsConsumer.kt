package settlements.obligations

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import settlements.obligations.topology.ApplyConfirmations
import settlements.obligations.topology.CreateOrUpdateObligationState
import settlements.obligations.topology.PersistObligations
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
    val streams = KafkaStreams(topology(), streamsConfiguration)
    streams.start()
    Runtime.getRuntime().addShutdownHook(Thread(streams::close))
  }

  companion object {
    fun topology(): Topology {
      val topology = Topology()
      topology.addStateStore(ObligationStateStore())

      PersistObligations(topology)
      CreateOrUpdateObligationState(topology)
      ApplyConfirmations(topology)
      
      topology.addSink("ObligationsState", "obligations-state", ApplyConfirmations.name, CreateOrUpdateObligationState.name)
      
      return topology
    }
  }
}

fun main(args: Array<String>) {
  ObligationsConsumer().run()
}