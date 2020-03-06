package com.maocq.semana0

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
 * 3. Encuentre ¿Cuáles son los números de vuelo (top 20)  que han tenido más cancelaciones y sus causas?
 *
 * Encuentre los vuelos más cancelados y cual es la causa mas frecuente
 * Un vuelo es cancelado si CANCELLED = 1
 * CANCELLATION_CODE A - Airline/Carrier; B - Weather; C - National Air System; D - Security
 */

object Reto3 extends App with Data {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def flightInfo(ds: DataFrame): Seq[CancelledFlight] = {
    val r = ds
      .filter($"CANCELLATION_CODE".isNotNull)
      .groupBy("OP_CARRIER_FL_NUM", "ORIGIN", "DEST")
      .agg(
        count("OP_CARRIER_FL_NUM").as("NUMBER"),
        collect_list("CANCELLATION_CODE").as("CANCELLATION_CODES")
      )
      .sort($"NUMBER".desc)
      .limit(20)
      .withColumn("CAUSES", transform($"CANCELLATION_CODES"))
      .as[CancelledFlight]

    r.show()
    r.collect().toSeq
  }

  val transform  = udf((array :mutable.WrappedArray[String]) => {
    array.map {
      case "A" => "Airline/Carrier"
      case "B" => "Weather"
      case "C" => "National Air System"
      case "D" => "Security"
      case x => x
    }.groupBy(identity).map(t => (t._1, t._2.size)).toList
  })

  val df = getDataFrame()
  flightInfo(df)
}

case class CancelledFlight(OP_CARRIER_FL_NUM: String, ORIGIN: String, DEST: String, NUMBER: Long, CAUSES: List[(String,Int)])
