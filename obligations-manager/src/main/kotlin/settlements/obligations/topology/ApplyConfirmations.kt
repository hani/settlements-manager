package settlements.obligations.topology

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.ProcessorSupplier
import settlements.obligations.ConfirmationProcessor
import settlements.obligations.ObligationStateStore

object ApplyConfirmations {
  const val name = "ApplyConfirmations"

  operator fun invoke(topology: Topology) {
    topology
        .addSource("confirmations-source", "confirmations")
        .addProcessor(
            name,
            ProcessorSupplier { ConfirmationProcessor() },
            "confirmations-source"
        )
    topology.connectProcessorAndStateStores(name, ObligationStateStore.name)
  }
}