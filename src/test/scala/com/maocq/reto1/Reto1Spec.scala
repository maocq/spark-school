package com.maocq.reto1

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.maocq.SparkSessionTestWrapper
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{count, sum}
import org.scalatest.FunSpec

class Reto1Spec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  describe("S4N") {
    it("Reto 1") {
      spark.sparkContext.setLogLevel("ERROR")

      val csv = spark.read.format("csv")
        .option("header", "true")
        .load(
          "/home/mauricio/Documentos/spark/spark/2009.csv",
          "/home/mauricio/Documentos/spark/spark/2010.csv"
        )

      val csvAirlines = spark.read.format("csv").option("header", "true")
        .load("/home/mauricio/Documentos/spark/spark/airlines.csv")

      val year: Option[String] = Option("2009")
      delayedAirlines(year, csv, csvAirlines)
    }
  }

  private def delayedAirlines(year: Option[String], csv: DataFrame, csvAirlines: DataFrame):  Seq[AirlineStats] = {
    val ds: Dataset[AirlineDelay] = csv.as[AirlineDelay]
    val dataset = year.map(y => ds.filter(r => r.FL_DATE.substring(0, 4) == y)).getOrElse(ds)

    val dataSetClasificacion = dataset.filter(_.ARR_DELAY.isDefined).map(r =>
      AirlineClassification(
        r.OP_CARRIER,
        r.ARR_DELAY.map(e => if (e.toDouble <= 5) 1 else 0),
        r.ARR_DELAY.map(e => if (e.toDouble > 5 && e.toDouble <= 45) 1 else 0),
        r.ARR_DELAY.map(e => if (e.toDouble > 45) 1 else 0)
      ))

    val temporalResult = dataSetClasificacion.groupBy("OP_CARRIER")
      .agg(
        count("OP_CARRIER").as("totalFlights"),
        sum("ON_TIME").as("onTimeFlights"),
        sum("SMALL_DELAY").as("smallDelayFlights"),
        sum("LARGE_DELAY").as("largeDelayFlights"))
      .sort($"largeDelayFlights".desc, $"smallDelayFlights".desc, $"smallDelayFlights".desc)

    val result = temporalResult.join(csvAirlines, $"OP_CARRIER" === $"IATA_CODE", "inner")
      .withColumnRenamed("AIRLINE", "name").as[AirlineStats]

    result.show()
    result.collect().toSeq
  }
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
