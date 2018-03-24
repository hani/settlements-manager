package settlements.generators

import io.kotlintest.properties.Gen
import settlements.Confirmation
import settlements.Direction

object ConfirmationGen : Gen<Confirmation> {

  override fun generate(): Confirmation =
      Confirmation(Gen.string().generate(),
          Gen.string().generate(),
          Gen.double().generate(),
          Gen.double().generate(),
          Gen.string().generate(),
          Gen.string().generate(),
          Gen.string().generate(),
          Gen.oneOf(Direction.values().toList()).generate()
      )
}
