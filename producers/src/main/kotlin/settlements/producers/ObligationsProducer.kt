package settlements.producers

import core.Currency
import core.epochDays
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import settlements.Direction
import settlements.Obligation
import java.time.LocalDate
import java.util.*
import kotlin.math.roundToInt

class ObligationsProducer {
  fun run() {
    val properties = Properties()
    properties.putAll(mapOf(
        "bootstrap.servers" to "localhost:19092",
        "acks" to "all",
        "linger.ms" to 1,
        "key.serializer" to StringSerializer::class.qualifiedName,
        "value.serializer" to KafkaAvroSerializer::class.qualifiedName,
        "schema.registry.url" to "http://localhost:8081"
    ))

    val producer = KafkaProducer<String, Obligation>(properties)
    (1..100).forEach {
      val obligation = Obligation(it.toString(),
          (Math.random() * 50000).roundToInt().toDouble(),
          Math.random() * 1_000_000,
          "US02100000",
          Currency.USD,
          Direction.DELIVER,
          LocalDate.now().plusDays(2).epochDays,
          System.nanoTime() / 1000
      )
      producer.send(ProducerRecord<String, Obligation>("obligations", obligation.id, obligation))
    }
    producer.close()
  }
}

fun main(args: Array<String>) {
  ObligationsProducer().run()
}