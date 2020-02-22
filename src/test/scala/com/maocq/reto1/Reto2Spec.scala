package com.maocq.reto1

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.maocq.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, udf}
import org.scalatest.FunSpec

class Reto2Spec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  describe("S4N") {
    it("Reto 2") {
      spark.sparkContext.setLogLevel("ERROR")

      val dataFrame = spark.read.format("csv").option("header", "true")
        .load(
          "/home/mauricio/Documentos/spark/spark/2009.csv",
          "/home/mauricio/Documentos/spark/spark/2010.csv"
        )

      val origin = "LGA"
      destinations(dataFrame, origin)
    }
  }

  private def destinations(dataFrame: DataFrame, origin: String): Seq[FlightsStats] = {
    val df = dataFrame
      .filter($"ORIGIN" === origin && $"DEP_TIME".isNotNull)

    def getDay: String => String = delay => delay.toDouble match {
      case x if x <= 800 => "morningFlights"
      case x if (x > 800 && x <= 1600) => "afternoonFlights"
      case _ => "nightFlights"
    }

    val dayFunction = udf(getDay)

    val result = df.withColumn("DAY", dayFunction($"DEP_TIME"))
      .groupBy("DEST")
      .pivot("DAY", Seq("morningFlights", "afternoonFlights", "nightFlights")).count()
      .na.fill(0)
      .sort("DEST")
      .withColumnRenamed("DEST", "destination")
      .as[FlightsStats]

    result.show()
    result.collect().toSeq
  }
}

case class FlightsStats(destination: String, morningFlights: Long, afternoonFlights: Long, nightFlights: Long)

