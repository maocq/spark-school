package com.maocq.semana0

import org.apache.spark.sql.DataFrame

/**
 * 1. ¿Cuáles son las aerolíneas más cumplidas y las menos cumplidas de un año en especifico?
 * La respuesta debe incluir el nombre completo de la aerolínea, si no se envia el año debe calcular con
 * toda la información disponible.
 *
 * Un vuelo se clasifica de la siguiente manera:
 * ARR_DELAY < 5 min --- On time
 * 5 > ARR_DELAY < 45min -- small Delay
 * ARR_DELAY > 45min large delay
 *
 * Calcule por cada aerolinea el número total de vuelos durante el año (en caso de no recibir el parametro de todos los años)
 * y el número de ontime flights, smallDelay flighst y largeDelay flights
 *
 * Orderne el resultado por largeDelayFlights, smallDelayFlightsy, ontimeFlights
 */

object Reto1 extends App with Data {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def delayedAirlines(year: Option[String], ds: DataFrame, airlines: DataFrame): Seq[AirlineStats] = {

    val r = ds
      .as[AirlineDelay]
      .filter(r => year.fold(true)(r.FL_DATE.startsWith))
      .filter(_.ARR_DELAY.isDefined)
      .map(r =>  AirlineClassification(
        r.OP_CARRIER,
        r.ARR_DELAY.map(e => if (e.toDouble <= 5) 1 else 0),
        r.ARR_DELAY.map(e => if (e.toDouble > 5 && e.toDouble <= 45) 1 else 0),
        r.ARR_DELAY.map(e => if (e.toDouble > 45) 1 else 0)))
      .groupBy("OP_CARRIER")
      .agg(
        count("OP_CARRIER").as("totalFlights"),
        sum("ON_TIME").as("onTimeFlights"),
        sum("SMALL_DELAY").as("smallDelayFlights"),
        sum("LARGE_DELAY").as("largeDelayFlights"))
      .sort($"largeDelayFlights".desc, $"smallDelayFlights".desc, $"smallDelayFlights".desc)
      .join(airlines, $"OP_CARRIER" === $"IATA_CODE", "inner")
      .withColumnRenamed("AIRLINE", "name")
      .as[AirlineStats]
    r.show()
    r.collect().toSeq
  }

  val year = Option("2009")
  val df = getDataFrame()
  val airlines = getAirlines()

  delayedAirlines(year, df, airlines)
}


case class AirlineDelay(FL_DATE: String, //Date of the flight, yy/mm/dd
                        OP_CARRIER: String, //Airline Identifier
                        ORIGIN: String, //Starting Airport Code
                        DEST: String, //Destination Airport Code
                        DEP_DELAY: Option[String], //Total Delay on Departure in minutes
                        ARR_DELAY: Option[String]) //Total Delay on Arrival in minutes

case class AirlineClassification(OP_CARRIER: String,
                                 ON_TIME: Option[Int],
                                 SMALL_DELAY: Option[Int],
                                 LARGE_DELAY: Option[Int])

case class AirlineStats(name: String,
                        totalFlights: Long,
                        largeDelayFlights: Long,
                        smallDelayFlights: Long,
                        onTimeFlights: Long)
