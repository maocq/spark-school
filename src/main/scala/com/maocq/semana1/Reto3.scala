package com.maocq.semana1

import java.time.LocalDate

import org.apache.spark.sql.DataFrame

/**
 * 3. De un listado de acciones,  listar el valor de apertura, de cierre, el valor mínimo y el valor máximo de la última semana
 * Para el resultado tener en cuenta: Mnemonic, StartPrice, EndPrice y Time
 *
 *  | Date | Trade | opening price | closing price| min price | max price |
 */

object Reto3 extends App  with Data {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def openingClosingPriceTrades(df: DataFrame, trades: Seq[String]): DataFrame = {
    val date = LocalDate.now().minusDays(7)
    df
      .filter(to_date($"Date").gt(lit(date.toString)))
      .filter($"Mnemonic".isin(trades:_*))
      .groupBy("Mnemonic")
      .agg(
        first("StartPrice").as("OpeningPrice"),
        last("EndPrice").as("ClosingPrice"),
        min("MinPrice").as("MinPrice"),
        max("MaxPrice").as("MaxPrice")
      )
  }

  val df = getDataFrame()
  val trades: Seq[String] = List("MIVB", "EUZ", "SZG", "SOW", "AHLA", "ACX", "2B7K", "EIN3", "BAYN")
  val r = openingClosingPriceTrades(df, trades)
  r.show()
}
