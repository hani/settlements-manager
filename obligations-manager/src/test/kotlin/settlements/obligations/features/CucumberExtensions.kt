package settlements.obligations.features

import core.Currency
import cucumber.api.DataTable
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.beanutils.PropertyUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import settlements.Direction
import settlements.SettlementStatus
import java.util.*
import kotlin.test.fail

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

fun publish(topic: String, records: List<Pair<String, SpecificRecord>>) {
  val producer = KafkaProducer<String, SpecificRecord>(KafkaProperties.producer(StepDefs.cluster.bootstrapServers()))
  records.map {
    producer.send(ProducerRecord(topic, it.first, it.second))
  }.forEach { it.get() }
  producer.close()
}

fun poll(topic: String, minResults: Int): List<SpecificRecord> {
  val accumilated = ArrayList<SpecificRecord>()
  val consumer = KafkaConsumer<String, SpecificRecord>(KafkaProperties.consumer(StepDefs.cluster.bootstrapServers()))
  consumer.seekToBeginning(Collections.emptyList())
  consumer.assign(listOf(TopicPartition(topic, 0)))
  val start = System.currentTimeMillis()
  while (accumilated.size < minResults && System.currentTimeMillis() - start < 5000) {
    accumilated.addAll(consumer.poll(5000L).map { it.value() })
  }
  consumer.close()
  if (accumilated.size < minResults) fail("Expected at least $minResults instead got ${accumilated.size}:\n${accumilated.joinToString("\n")}")
  return accumilated
}

val DataTable.rows: List<Map<String, String>>
  get() = this.asMaps(String::class.java, String::class.java)
