package settlements.obligations.topology

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.ProcessorSupplier
import settlements.obligations.ConfirmationProcessor
import settlements.obligations.ObligationStateStore

object ApplyConfirmations {
  const val name = "ApplyConfirmations"
  private const val sourceName = "confirmations-source"

  operator fun invoke(topology: Topology) {
    topology
        .addSource(sourceName, Topics.Confirmations)
        .addProcessor(
            name,
            ProcessorSupplier { ConfirmationProcessor() },
            sourceName
        )
    topology.connectProcessorAndStateStores(name, ObligationStateStore.name)
  }
}