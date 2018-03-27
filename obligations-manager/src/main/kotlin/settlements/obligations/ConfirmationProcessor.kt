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
    store = context.getStateStore(ObligationStateStore.name) as ReadOnlyKeyValueStore<String, ObligationState>
  }

  override fun process(key: String, confirmation: Confirmation) {
    val state = store?.get(confirmation.obligationId)
    if(state != null) {
      state.openAmount = state.openAmount - confirmation.amount
      state.openQuantity = state.openQuantity - confirmation.quantity
      when {
        state.openQuantity.toInt() == 0 -> state.status = SettlementStatus.FULLY_SETTLED
        state.openQuantity == state.obligation.quantity -> state.status = SettlementStatus.OPEN
        state.openQuantity < state.obligation.quantity -> state.status = SettlementStatus.PARTIALLY_SETTLED
      }
      context().forward(confirmation.obligationId, state)
      context().commit()
    }
  }
}