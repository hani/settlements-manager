package settlements.obligations.topology

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.internals.KTableSource
import settlements.ObligationState
import settlements.obligations.ObligationStateStore

object PersistObligations {
  const val name = "StoreObligationState"
  private const val sourceName = "obligations-state-source"
  
  operator fun invoke(topology: Topology) {
    topology
        .addSource(sourceName, Topics.ObligationState)
        .addProcessor(
            name,
            KTableSource<String, ObligationState>(ObligationStateStore.name),
            sourceName
        )
    topology.connectProcessorAndStateStores(name, ObligationStateStore.name)
  }
}