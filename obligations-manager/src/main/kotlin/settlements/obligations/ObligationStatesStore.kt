package settlements.obligations

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.Stores
import settlements.ObligationState

object ObligationStatesStore {
  
  const val name = "ObligationsStateStore"
  
  operator fun invoke() = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(name),
      Serdes.String(),
      SpecificAvroSerde<ObligationState>()
  ).withCachingEnabled()!! 
}