package core

import java.time.LocalDate
import java.time.temporal.ChronoUnit

val LocalDate.epochDays: Int
  get() = ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), this).toInt()

val Int.asLocalDate: LocalDate
  get() = ChronoUnit.DAYS.addTo(LocalDate.ofEpochDay(0), this.toLong())