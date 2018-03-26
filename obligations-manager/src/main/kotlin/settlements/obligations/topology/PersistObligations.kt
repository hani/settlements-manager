package settlements.obligations.topology

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.internals.KTableSource
import settlements.ObligationState
import settlements.obligations.ObligationStateStore

object PersistObligations {
  const val name = "StoreObligationState"
  
  operator fun invoke(topology: Topology) {
    topology
        .addSource("obligations-state-source", Topics.ObligationState)
        .addProcessor(
            name,
            KTableSource<String, ObligationState>(ObligationStateStore.name),
            "obligations-state-source"
        )
    topology.connectProcessorAndStateStores(name, ObligationStateStore.name)
  }
}