package settlements.obligations.features

import cucumber.api.DataTable
import cucumber.api.java8.En
import io.confluent.kafka.streams.serdes.avro.InMemorySpecificAvroSerde
import io.kotlintest.matchers.shouldEqual
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.beanutils.PropertyUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.test.ProcessorTopologyTestDriver
import org.slf4j.LoggerFactory
import settlements.Confirmation
import settlements.Obligation
import settlements.ObligationState
import settlements.generators.ConfirmationGen
import settlements.generators.ObligationGen
import settlements.obligations.ObligationStateStore
import settlements.obligations.ObligationsConsumer
import settlements.obligations.SettlementsSerdes
import java.util.*

class StepDefs : En {

  private val log = LoggerFactory.getLogger(StepDefs::class.java)

  init {
    val properties = Properties()
    var driver: ProcessorTopologyTestDriver? = null

    properties.putAll(mapOf(
        "bootstrap.servers" to "dummy",
        "application.id" to "filter",
        "auto.offset.reset" to "earliest",
        "schema.registry.url" to "http://ignored_for_inmemory",
        StreamsConfig.STATE_DIR_CONFIG to "data",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to InMemorySpecificAvroSerde::class.qualifiedName
    ))
    val registry = InMemorySpecificAvroSerde.SchemaRegistryClient
    registry.register("obligations-value", Obligation().schema)
    registry.register("confirmations-value", Confirmation().schema)
    registry.register("obligations-state-value", ObligationState().schema)

    Before { _ ->
      driver = ProcessorTopologyTestDriver(StreamsConfig(properties), ObligationsConsumer.topology())
    }

    After { _ ->
      driver?.close()
    }

    fun publish(topic: String, records: List<Pair<String, SpecificRecord>>) {
      records.forEach {
        driver?.process(topic, it.first, it.second,
            StringSerializer(), InMemorySpecificAvroSerde<SpecificRecord>().serializer())
      }
    }

    Given("^the following new obligations:$") { table: DataTable ->
      val obligations = table.rows.map {
        val obligation = ObligationGen.generate()
        obligation.setProperties(it)
        obligation
      }
      publish("obligations", obligations.map { it.id to it })
    }

    Then("^the obligation state store should contain:$") { table: DataTable ->
      val actual = driver?.getKeyValueStore<String, ObligationState>(ObligationStateStore.name)?.all()?.asSequence()
      actual?.count() shouldEqual table.rows.size
      actual?.zip(table.rows.asSequence())?.forEach { (actual, expected) ->
        val actualMap = PropertyUtils.describe(actual!!.value)
        expected.forEach { 
          actualMap[it.key] shouldEqual it.value
        }
      }
    }
    
    Given("^the following confirmations are received:$") { table: DataTable ->
      val confirmations = table.rows.map {
        val confirmation = ConfirmationGen.generate()
        confirmation.setProperties(it)
        confirmation
      }
      publish("confirmations", confirmations.map { it.id to it })
    }

    Then("^the following obligation states are published:$") { table: DataTable ->
      table.rows.forEach {
        val output = driver?.readOutput("obligations-state", Serdes.String().deserializer(), SettlementsSerdes.obligationState(registry).deserializer())
        val actual = PropertyUtils.describe(output!!.value())
        it.forEach {
          actual[it.key].toString() shouldEqual it.value
        }
      }
    }
  }
}