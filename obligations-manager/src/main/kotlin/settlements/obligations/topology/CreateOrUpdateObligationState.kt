package settlements.obligations.topology

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.ProcessorSupplier
import settlements.obligations.ObligationProcessor
import settlements.obligations.ObligationStateStore

object CreateOrUpdateObligationState {
  const val name = "CreateOrUpdateObligation"
  private const val sourceName = "obligations-source"
  
  operator fun invoke(topology: Topology) {
    topology
        .addSource(sourceName, Topics.Obligations)
        .addProcessor(name,
            ProcessorSupplier { ObligationProcessor() },
            sourceName
        )
    topology.connectProcessorAndStateStores(name, ObligationStateStore.name)
  }
}