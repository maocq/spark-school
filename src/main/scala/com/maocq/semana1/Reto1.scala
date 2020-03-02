package com.maocq.semana1

import java.time.LocalDate

import org.apache.spark.sql.DataFrame

/**
 * 1. ¿Cuales son las diferentes acciones de los últimos 6 meses?, ¿cuál es su moneda(currency) y cúal es su valor promedio?
 * Para el resultado tener en cuenta: Mnemonic, Currency, avg(EndPrice)
 */

object Reto1 extends App with Data {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def last6monthTrades(df: DataFrame): DataFrame = {
    val date = LocalDate.now().minusMonths(6)

    df
      .filter(to_date($"Date").gt(lit(date.toString)))
      .groupBy("Mnemonic", "Currency")
      .agg(avg("EndPrice").as("Avg"))
      .sort($"Avg".desc)
  }

  val df = getDataFrame()
  val r = last6monthTrades(df)
  r.show()
}
