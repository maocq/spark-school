package com.maocq.reto1


import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.maocq.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSpec

class Reto4Spec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer  {
  import spark.implicits._

  describe("S4N") {
    it("Reto 4") {
      spark.sparkContext.setLogLevel("ERROR")

      val dataFrame = spark.read.format("csv")
        .option("header", "true")
        .load(
          "/home/mauricio/Documentos/spark/spark/2009.csv",
          "/home/mauricio/Documentos/spark/spark/2010.csv"
        )

      daysWithDelays(dataFrame)
    }
  }

  private def daysWithDelays(dataFrame: DataFrame): List[(String, Long)] = {
    import org.apache.spark.sql.functions.udf
    def dateToDay: String => String = { date =>
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-d")
      LocalDate.parse(date, formatter).getDayOfWeek.toString
    }

    val toDay = udf(dateToDay)

    val resultado = dataFrame
      .filter($"ARR_DELAY".isNotNull && $"ARR_DELAY" > 45)
      .withColumn("day", toDay($"FL_DATE"))
      .groupBy("day")
      .count()
      .sort($"count".desc).as[(String, Long)]

    resultado.show()
    resultado.collect().toList
  }
}
