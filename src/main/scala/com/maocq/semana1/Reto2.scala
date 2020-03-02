package com.maocq.semana1

import org.apache.spark.sql.DataFrame

/**
 * 2. Buscar en los últimos tres meses cuáles acciones con más ventas y el precio promedio por cada mes
 *  Filtrar del contenido de los archivos las lineas donde NumberOfTrades > 50
 *
 *  | Trade | currency | Dec-19 | Jan-20 | Feb-20 |
 *  |-------|----------|--------|--------|--------|
 */

object Reto2 extends App with Data {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def lastThreeMonthTradesAveragePrice(df: DataFrame): DataFrame = df
    .filter($"NumberOfTrades" > 50)
    .withColumn("Mounth", date_format($"Date", "MMM-yyyy"))
    .groupBy("Mnemonic", "Currency")
    .pivot("Mounth")
    .agg(
      avg("EndPrice")
    )

  val df = getDataFrame()
  val r = lastThreeMonthTradesAveragePrice(df)
  r.show()
}
