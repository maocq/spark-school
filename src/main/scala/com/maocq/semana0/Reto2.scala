package com.maocq.semana0

import org.apache.spark.sql.DataFrame


/**
 * 2. Dado un origen por ejemplo DCA (Washington), ¿Cuáles son destinos y cuantos vuelos presentan durante la mañana, tarde y noche?
 *
 * Encuentre los destinos a partir de un origen, y de acuerdo a DEP_TIME clasifique el vuelo de la siguiente manera:
 * 00:00 y 8:00 - Morning
 * 8:01 y 16:00 - Afternoon
 * 16:01 y 23:59 - Night
 */

object Reto2 extends App with Data {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def destinations(ds: DataFrame, origin: String): Seq[FlightsStats] = {
    val r = ds
      .filter($"ORIGIN" === origin && $"DEP_TIME".isNotNull)
      .withColumn("DAY", dayFunction($"DEP_TIME"))
      .groupBy("DEST")
      .pivot("DAY", Seq("morningFlights", "afternoonFlights", "nightFlights")).count()
      .na.fill(0)
      .sort("DEST")
      .withColumnRenamed("DEST", "destination")
      .as[FlightsStats]

    r.show()
    r.collect().toSeq
  }

  val dayFunction  = udf((value: Double) => value match {
    case x if x <= 800 => "morningFlights"
    case x if (x > 800 && x <= 1600) => "afternoonFlights"
    case _ => "nightFlights"
  })

  val df = getDataFrame()
  val origin = "LGA"
  destinations(df, origin)
}

case class FlightsStats(destination: String, morningFlights: Long, afternoonFlights: Long, nightFlights: Long)
