package settlements.generators

import core.Currency
import io.kotlintest.properties.Gen
import io.kotlintest.properties.map
import settlements.Direction
import settlements.Obligation

object ObligationGen : Gen<Obligation> {

  override fun generate(): Obligation =
      Obligation(Gen.string().generate(),
          Gen.double().generate(),
          Gen.double().generate(),
          Gen.string().generate(),
          Gen.oneOf(Currency.values().toList()).generate(),
          Gen.oneOf(Direction.values().toList()).generate(),
          Gen.positiveIntegers().generate(),
          Gen.positiveIntegers().map { it * 10000L }.generate()
      )
}
