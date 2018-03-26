package settlements.obligations

import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import settlements.Confirmation
import settlements.ObligationState
import settlements.SettlementStatus

class ConfirmationProcessor : AbstractProcessor<String, Confirmation>() {

  private var store: ReadOnlyKeyValueStore<String, ObligationState>? = null

  override fun init(context: ProcessorContext) {
    super.init(context)
    @Suppress("UNCHECKED_CAST")
    store = context.getStateStore(ObligationStatesStore.name) as ReadOnlyKeyValueStore<String, ObligationState>
  }

  override fun process(key: String, confirmation: Confirmation) {
    val existing = store?.get(key)
    println("Processing $confirmation")
    val updated = if(existing != null) {
      existing.openAmount = existing.openAmount - confirmation.amount
      existing.openQuantity = existing.openQuantity - confirmation.quantity
      if (existing.openQuantity.toInt() == 0) existing.status = SettlementStatus.FULLY_SETTLED
      else if (existing.openQuantity < existing.obligation.quantity) existing.status = SettlementStatus.PARTIALLY_SETTLED
      existing
    } else null
    context().forward(key, updated)
    context().commit()
  }
}