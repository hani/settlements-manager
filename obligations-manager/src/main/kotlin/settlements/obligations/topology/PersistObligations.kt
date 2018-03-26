package settlements.obligations.topology

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.internals.KTableSource
import settlements.ObligationState
import settlements.obligations.ObligationStatesStore

object PersistObligations {
  operator fun invoke(topology: Topology) {
    topology
        .addSource("obligations-state-source", "obligation-state")
        .addProcessor(
            "StoreObligationState",
            KTableSource<String, ObligationState>(ObligationStatesStore.name),
            "obligations-state-source"
        )
    topology.connectProcessorAndStateStores("StoreObligationState", ObligationStatesStore.name)
  }
}