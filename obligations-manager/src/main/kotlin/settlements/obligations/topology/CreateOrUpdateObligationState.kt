package settlements.obligations.topology

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.ProcessorSupplier
import settlements.obligations.ObligationProcessor
import settlements.obligations.ObligationStateStore

object CreateOrUpdateObligationState {
  const val name = "CreateOrUpdateObligation"
  
  operator fun invoke(topology: Topology) {
    topology
        .addSource("obligations-source", Topics.Obligations)
        .addProcessor(name,
            ProcessorSupplier { ObligationProcessor() },
            "obligations-source"
        )
    topology.connectProcessorAndStateStores(name, ObligationStateStore.name)
  }
}