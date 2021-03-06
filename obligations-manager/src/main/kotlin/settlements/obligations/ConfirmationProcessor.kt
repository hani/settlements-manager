package settlements.obligations

import mu.KLogging
import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import settlements.Confirmation
import settlements.ObligationState
import settlements.SettlementStatus

class ConfirmationProcessor : AbstractProcessor<String, Confirmation>() {

  companion object : KLogging()
  
  private var store: KeyValueStore<String, ObligationState>? = null

  override fun init(context: ProcessorContext) {
    super.init(context)
    @Suppress("UNCHECKED_CAST")
    store = context.getStateStore(ObligationStateStore.name) as KeyValueStore<String, ObligationState>
  }

  override fun process(key: String, confirmation: Confirmation) {
    val state = store?.get(confirmation.obligationId)
    logger.info("Obligation for confirmation: $state")
    if(state != null) {
      state.openAmount = state.openAmount - confirmation.amount
      state.openQuantity = state.openQuantity - confirmation.quantity
      when {
        state.openQuantity.toInt() == 0 -> state.status = SettlementStatus.FULLY_SETTLED
        state.openQuantity == state.obligation.quantity -> state.status = SettlementStatus.OPEN
        state.openQuantity < state.obligation.quantity -> state.status = SettlementStatus.PARTIALLY_SETTLED
      }
      logger.info("Sending $state")
      store?.put(confirmation.obligationId, state)
      context().forward(confirmation.obligationId, state)
    }
  }
}