package settlements.obligations

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Produced
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
        StreamsConfig.STATE_DIR_CONFIG to "data"
    ))
    val builder = topology(CachedSchemaRegistryClient("http://localhost:8081", 1000))
    KafkaStreams(builder, streamsConfiguration).start()
  }
  
  companion object {
    fun topology(client: SchemaRegistryClient): Topology {
      val builder = StreamsBuilder()
      val obligations = builder.stream<String, Obligation>("obligations", Consumed.with(Serdes.String(), SettlementsSerdes.obligation(client)))
      val obligationsState = builder.table<String, ObligationState>("obligations-state", Consumed.with(Serdes.String(), SettlementsSerdes.obligationState(client)))
      obligations.leftJoin(obligationsState, { obligation, state ->
        if (state == null) {
          ObligationState(obligation.id, obligation, SettlementStatus.OPEN, obligation.quantity, obligation.amount)
        } else {
          state.obligation = obligation
          state
        }
      }).to("obligations-state", Produced.with(Serdes.String(), SettlementsSerdes.obligationState(client)))
      return builder.build()
    }
  }
}

fun main(args: Array<String>) {
  ObligationsConsumer().run()
}