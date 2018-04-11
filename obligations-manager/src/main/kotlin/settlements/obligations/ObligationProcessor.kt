package settlements.obligations

import mu.KLogging
import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import settlements.Obligation
import settlements.ObligationState
import settlements.SettlementStatus

class ObligationProcessor : AbstractProcessor<String, Obligation>() {

  companion object : KLogging()

  private var store: KeyValueStore<String, ObligationState>? = null

  override fun init(context: ProcessorContext) {
    super.init(context)
    @Suppress("UNCHECKED_CAST")
    store = context.getStateStore(ObligationStateStore.name) as KeyValueStore<String, ObligationState>
  }

  override fun process(key: String, obligation: Obligation) {
    val existing = store?.get(key)
    logger.info("existing: $existing. new: $obligation")
    val updated = if (existing == null) {
      ObligationState(obligation.id, obligation, SettlementStatus.OPEN, obligation.quantity, obligation.amount)
    } else {
      existing.openQuantity = existing.openQuantity - (existing.obligation.quantity - obligation.quantity)
      when {
        existing.openQuantity == obligation.quantity -> existing.status = SettlementStatus.OPEN
        existing.openQuantity.toInt() == 0 -> existing.status = SettlementStatus.FULLY_SETTLED
        existing.openQuantity > 0 -> existing.status = SettlementStatus.PARTIALLY_SETTLED
      }

      existing.openAmount = existing.openAmount - (existing.obligation.amount - obligation.amount)

      existing.obligation = obligation
      existing
    }
    store?.put(key, updated)
    context().forward(key, updated)
  }
}