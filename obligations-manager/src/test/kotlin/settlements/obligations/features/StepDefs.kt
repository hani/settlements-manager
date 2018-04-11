package settlements.obligations.features

import cucumber.api.DataTable
import cucumber.api.java8.En
import io.confluent.kafka.streams.serdes.avro.InMemorySpecificAvroSerde
import io.kotlintest.matchers.shouldEqual
import mu.KLogging
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.beanutils.PropertyUtils
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
import org.apache.kafka.streams.state.QueryableStoreTypes
import settlements.ObligationState
import settlements.generators.ConfirmationGen
import settlements.generators.ObligationGen
import settlements.obligations.ObligationStateStore
import settlements.obligations.ObligationsConsumer
import settlements.obligations.topology.Topics
import java.io.File
import kotlin.test.assertEquals

class StepDefs : En {

  companion object : KLogging(){
    val cluster = EmbeddedKafkaCluster(1)

    init {
      cluster.start()
    }
  }

  private val appId = "cucumber-test"
  private val changeLogTopic = "$appId-ObligationsStateStore-changelog"

  private val schemaUrl = "http://ignored_for_inmemory"

  init {
    var streams: KafkaStreams? = null

    InMemorySpecificAvroSerde.SchemaRegistryClient.register("$changeLogTopic-value", ObligationState.`SCHEMA$`)
    Before { _ ->
      File("data").deleteRecursively()
      cluster.deleteAndRecreateTopics(
          Topics.ObligationState,
          Topics.Confirmations,
          Topics.Obligations,
          changeLogTopic
      )
      streams = KafkaStreams(ObligationsConsumer.topology(InMemorySpecificAvroSerde<SpecificRecord>(), schemaUrl),
          KafkaProperties.streams(appId, schemaUrl, cluster.bootstrapServers()))
      streams?.start()
    }

    After { _ ->
      streams?.close()
      File("data").deleteRecursively()
    }

    fun waitForRunning() {
      while (streams?.state() != KafkaStreams.State.RUNNING) {
        logger.info("current state ${streams?.state()}")
        Thread.sleep(100)
      }
    }
    
    Given("^the following obligations:$") { table: DataTable ->
      val obligations = table.rows.map {
        val obligation = ObligationGen.generate()
        obligation.setProperties(it)
        obligation
      }
      publish(Topics.Obligations, obligations.map { it.id to it })
    }

    Then("^the obligation state store should contain:$") { table: DataTable ->
      waitForRunning()
      val store = streams?.store(ObligationStateStore.name, QueryableStoreTypes.keyValueStore<String, ObligationState>())
      val actual = store?.all()?.asSequence()
      actual?.zip(table.rows.asSequence())?.forEach { (actual, expected) ->
        val actualMap = PropertyUtils.describe(actual!!.value).filterNot { it.key == "schema" }
        expected.forEach {
          assertEquals(it.value, actualMap[it.key].toString(), "actual map: $actualMap")
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
      val actual = poll(Topics.ObligationState, table.rows.size)
      actual.size shouldEqual table.rows.size
      actual.zip(table.rows).forEach { (actual, expected) ->
        val actualMap = PropertyUtils.describe(actual)
        expected.forEach {
          actualMap[it.key].toString() shouldEqual it.value
        }
      }
    }
  }
}