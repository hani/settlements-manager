package settlements.obligations

import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import settlements.Obligation
import settlements.ObligationState
import settlements.SettlementStatus

class ObligationProcessor : AbstractProcessor<String, Obligation>() {

  private var store: ReadOnlyKeyValueStore<String, ObligationState>? = null

  override fun init(context: ProcessorContext) {
    super.init(context)
    @Suppress("UNCHECKED_CAST")
    store = context.getStateStore(ObligationStateStore.name) as ReadOnlyKeyValueStore<String, ObligationState>
  }

  override fun process(key: String, obligation: Obligation) {
    println("Processing $obligation")
    val existing = store?.get(key)
    val updated = if(existing == null) {
      ObligationState(obligation.id, obligation, SettlementStatus.OPEN, obligation.quantity, obligation.amount)
    } else {
      existing.obligation = obligation
      existing
    }
    context().forward(key, updated)
    context().commit()
  }
}