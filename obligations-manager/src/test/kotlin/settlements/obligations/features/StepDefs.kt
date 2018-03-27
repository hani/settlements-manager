package settlements.obligations.features

import cucumber.api.DataTable
import cucumber.api.java8.En
import io.confluent.kafka.streams.serdes.avro.InMemoryKafkaAvroDeserializer
import io.confluent.kafka.streams.serdes.avro.InMemoryKafkaAvroSerializer
import io.confluent.kafka.streams.serdes.avro.InMemorySpecificAvroSerde
import io.kotlintest.matchers.shouldEqual
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.beanutils.PropertyUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
import org.apache.kafka.streams.state.QueryableStoreTypes
import settlements.ObligationState
import settlements.generators.ConfirmationGen
import settlements.generators.ObligationGen
import settlements.obligations.ObligationStateStore
import settlements.obligations.ObligationsConsumer
import settlements.obligations.topology.Topics
import java.io.File
import java.util.*

class StepDefs : En {

  companion object {
    val cluster = EmbeddedKafkaCluster(1)
    init {
      cluster.start()
    }
  }
  
  private val appId = "cucumber-test"
  private val schemaUrl = "http://ignored_for_inmemory"
  
  init {
    var streams: KafkaStreams? = null
    val properties = Properties()

    properties.putAll(mapOf(
        "bootstrap.servers" to cluster.bootstrapServers(),
        "application.id" to appId,
        "auto.offset.reset" to "earliest",
        "schema.registry.url" to schemaUrl,
        StreamsConfig.STATE_DIR_CONFIG to "data",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to InMemorySpecificAvroSerde::class.qualifiedName
    ))

    InMemorySpecificAvroSerde.SchemaRegistryClient.register("$appId-ObligationsStateStore-changelog-value", ObligationState.`SCHEMA$`)
    Before { _ ->
      File("data").deleteRecursively()
      cluster.deleteAndRecreateTopics(Topics.ObligationState, Topics.Confirmations, Topics.Obligations)
      streams = KafkaStreams(ObligationsConsumer.topology(InMemorySpecificAvroSerde<SpecificRecord>(), schemaUrl), properties)
      streams?.start()
    }

    After { _ ->
      streams?.close()
      File("data").deleteRecursively()
    }

    fun publish(topic: String, records: List<Pair<String, SpecificRecord>>) {
      val producer = KafkaProducer<String, SpecificRecord>(mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to cluster.bootstrapServers(),
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.RETRIES_CONFIG to 0,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                "schema.registry.url" to "http://ignored_for_inmemory",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to InMemoryKafkaAvroSerializer::class.java
            ))
      records.map { 
        producer.send(ProducerRecord(topic, it.first, it.second))
      }.forEach { it.get() }
      producer.close()
    }

    Given("^the following new obligations:$") { table: DataTable ->
      val obligations = table.rows.map {
        val obligation = ObligationGen.generate()
        obligation.setProperties(it)
        obligation
      }
      publish(Topics.Obligations, obligations.map { it.id to it })
    }

    Then("^the obligation state store should contain:$") { table: DataTable ->
      val store = streams?.store(ObligationStateStore.name, QueryableStoreTypes.keyValueStore<String, ObligationState>())
      val actual = store?.all()?.asSequence()
      actual?.zip(table.rows.asSequence())?.forEach { (actual, expected) ->
        val actualMap = PropertyUtils.describe(actual!!.value)
        expected.forEach {
          actualMap[it.key].toString() shouldEqual it.value
        }
      }
      store?.all()?.asSequence()?.count() shouldEqual table.rows.size
    }

    Given("^the following confirmations are received:$") { table: DataTable ->
      val confirmations = table.rows.map {
        val confirmation = ConfirmationGen.generate()
        confirmation.setProperties(it)
        confirmation
      }
      publish(Topics.Confirmations, confirmations.map { it.id to it })
    }

    Then("^the following obligation states are published:$") { table: DataTable ->
      val config = Properties()
      config.putAll(mapOf(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to cluster.bootstrapServers(),
          ConsumerConfig.GROUP_ID_CONFIG to "cucumber-consumer",
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to InMemoryKafkaAvroDeserializer::class.java,
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
          "specific.avro.reader" to true,
          "schema.registry.url" to "http://ignored_for_inmemory"
      ))
      val consumer = KafkaConsumer<String, ObligationState>(config)
      consumer.seekToBeginning(Collections.emptyList())
      consumer.assign(listOf(TopicPartition(Topics.ObligationState, 0)))
      val actual = consumer.poll(5000L).map { it.value() }
      actual.zip(table.rows).forEach { (actual, expected) ->
        val actualMap = PropertyUtils.describe(actual!!)
        expected.forEach {
          actualMap[it.key].toString() shouldEqual it.value
        }
      }
    }
  }
}