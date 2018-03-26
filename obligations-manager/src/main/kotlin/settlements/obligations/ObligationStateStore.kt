package settlements.obligations

import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.state.Stores

object ObligationStateStore {
  
  const val name = "ObligationsStateStore"
  
  operator fun invoke(serde: Serde<SpecificRecord>) = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(name),
      Serdes.String(),
      serde
  ).withCachingEnabled()!! 
}