package com.maocq.reto1

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.maocq.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSpec

import scala.collection.mutable

case class CancelledFlight(OP_CARRIER_FL_NUM: String, ORIGIN: String, DEST: String, NUMBER: Long, CAUSES: List[(String,Int)])

class Reto3Spec  extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  describe("S4N") {
    it("Reto 3") {
      spark.sparkContext.setLogLevel("ERROR")

      val dataFrame = spark.read.format("csv")
        .option("header", "true")
        .load(
          "/home/mauricio/Documentos/spark/spark/2009.csv",
          "/home/mauricio/Documentos/spark/spark/2010.csv"
        )

      flightInfo(dataFrame)
    }
  }

  private def flightInfo(dataFrame: DataFrame): Seq[CancelledFlight] = {
    import org.apache.spark.sql.functions.udf
    def transform: mutable.WrappedArray[String] => List[(String, Int)] = { array =>
      val causes = Map("A" -> "Airline/Carrier", "B" -> "Weather", "C" -> "National Air System", "D" -> "Security")
      array.groupBy(identity).map(s => (causes.getOrElse(s._1, s._1), s._2.size)).toList
    }
    val transformFuntion = udf(transform)

    import org.apache.spark.sql.functions._
    val df = dataFrame
      .filter($"CANCELLATION_CODE".isNotNull)
      .groupBy("OP_CARRIER_FL_NUM", "ORIGIN", "DEST")
      .agg(
        count("OP_CARRIER_FL_NUM").as("NUMBER"),
        collect_list("CANCELLATION_CODE").as("CANCELLATION_CODES")
      ).sort($"NUMBER".desc).limit(20).withColumn("CAUSES", transformFuntion($"CANCELLATION_CODES"))
      .as[CancelledFlight]

    df.show()
    df.collect().toSeq
  }
}

