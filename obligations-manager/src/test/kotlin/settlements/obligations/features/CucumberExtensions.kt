package settlements.obligations.features

import core.Currency
import cucumber.api.DataTable
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.beanutils.PropertyUtils
import settlements.Direction
import settlements.SettlementStatus

fun SpecificRecord.setProperties(values: Map<String, String>) {
  values.map {
    val targetType = PropertyUtils.getPropertyType(this, it.key)
    val value: Any = when (targetType) {
      java.lang.Long::class.java -> it.value.toLong()
      java.lang.Integer::class.java -> it.value.toInt()
      java.lang.Double::class.java -> it.value.toDouble()
      Currency::class.java -> Currency.valueOf(it.value)
      Direction::class.java -> Direction.valueOf(it.value)
      SettlementStatus::class.java -> SettlementStatus.valueOf(it.value)
      else -> it.value
    }
    PropertyUtils.setProperty(this, it.key, value)
  }
}

val DataTable.rows: List<Map<String, String>>
  get() = this.asMaps(String::class.java, String::class.java)
