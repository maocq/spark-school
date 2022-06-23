package com.maocq.semana0

import org.apache.spark.sql.DataFrame

/**
 * 4. ¿Que dias se presentan más retrasos históricamente?
 *
 * Encuentre que dia de la semana se presentan más demoras,
 * sólo tenga en cuenta los vuelos donde ARR_DELAY > 45min
 */

object Reto4 extends App with Data {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def daysWithDelays(ds: DataFrame): List[(String, Long)] = {
    val r = ds
      .filter($"ARR_DELAY".isNotNull && $"ARR_DELAY" > 45)
      .withColumn("day", date_format($"FL_DATE", "E" ))
      .groupBy("day")
      .count()
      .sort($"count".desc).as[(String, Long)]

    r.show()
    r.collect().toList
  }

  val df = getDataFrame()
  daysWithDelays(df)
}
