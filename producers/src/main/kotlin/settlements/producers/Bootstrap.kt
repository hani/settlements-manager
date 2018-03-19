package settlements.producers

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory
import java.util.*

fun main(args: Array<String>) {
  createTopics()
}

fun createTopics() {
  val log = LoggerFactory.getLogger("bootstrap")
  
  val properties = Properties()
  properties.putAll(mapOf(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:19092"
  ))
  val client = AdminClient.create(properties)
  val result = client.createTopics(listOf(
      NewTopic("obligations", 3, 2),
      NewTopic("instructions", 3, 2),
      NewTopic("confirmations", 3, 2),
      NewTopic("obligations-state", 3, 2)
  ))
  for (entry in result.values().entries) {
    try {
      entry.value.get()
      log.info("Created topic ${entry.key}")
    } catch (e: Exception) {
      log.info("Error creating topic ${entry.key}: ${e.message}")
    }
  }
}