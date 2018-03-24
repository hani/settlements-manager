package settlements.obligations

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import settlements.Obligation
import settlements.ObligationState
import settlements.SettlementStatus
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
      val builder = StreamsBuilder()
      val obligations = builder.stream<String, Obligation>("obligations")
      val obligationsState = builder.table<String, ObligationState>("obligations-state")
      obligations.leftJoin(obligationsState, { obligation, state ->
        if (state == null) {
          ObligationState(obligation.id, obligation, SettlementStatus.OPEN, obligation.quantity, obligation.amount)
        } else {
          state.obligation = obligation
          state
        }
      }).to("obligations-state")
      return builder.build()
    }
  }
}

fun main(args: Array<String>) {
  ObligationsConsumer().run()
}