package settlements.obligations.features

import cucumber.api.DataTable
import cucumber.api.java8.En
import io.confluent.kafka.streams.serdes.avro.InMemorySpecificAvroSerde
import io.kotlintest.matchers.shouldEqual
import org.apache.commons.beanutils.PropertyUtils
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.test.ProcessorTopologyTestDriver
import org.slf4j.LoggerFactory
import settlements.Obligation
import settlements.ObligationState
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
    registry.register("obligations-state-value", ObligationState().schema)
    registry.register("test-driver-application-obligations-state-STATE-STORE-0000000001-changelog", ObligationState().schema)
    registry.register("test-driver-application-obligations-state-STATE-STORE-0000000001-changelog-value", ObligationState().schema)

    Before { _ ->
      driver = ProcessorTopologyTestDriver(StreamsConfig(properties), ObligationsConsumer.topology())
    }

    After { _ ->
      driver?.close()
    }

    fun publish(topic: String, obligations: List<Obligation>) {
      obligations.forEach {
        driver?.process(topic, it.id, it,
            Serdes.String().serializer(),
            SettlementsSerdes.obligation(registry).serializer())
      }
    }

    Given("^The following new obligations:$") { table: DataTable ->
      val obligations = table.rows.map {
        val obligation = Obligation()
        obligation.setProperties(it)
        obligation
      }
      publish("obligations", obligations)
    }

    Then("^The following obligation states are published:$") { table: DataTable ->
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