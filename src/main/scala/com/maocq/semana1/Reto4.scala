package com.maocq.semana1

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

/**
 *  4. Obtenga la tendencia de una acciones en los últimos dias con respecto al día anterior,
 *  de esta manera determinar si el valor subió, bajó o se mantuvo.
 *  Para el resultado tener en cuenta: Mnemonic, avg(EndPrice)
 */

object Reto4 extends App with Data {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def tradeTrending(df: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("Mnemonic")
      .orderBy("Date", "Time")

    df.withColumn("Behavior",
      behaviorFunction('EndPrice - lag('EndPrice, 1, 0).over(windowSpec)))
  }

  val behaviorFunction  = udf((value: Double) => value match {
    case x if x > 0 => "SUBIO"
    case x if x < 0 => "BAJO"
    case _ => "IGUAL"
  })

  val df = getDataFrame()
  val r = tradeTrending(df)
  r.show()
}


