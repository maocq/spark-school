package com.maocq.semana0

import com.maocq.SparkSessionWrapper
import org.apache.spark.sql.DataFrame

/**
 * https://www.kaggle.com/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018
 * https://www.kaggle.com/usdot/flight-delays
 */

trait Data extends SparkSessionWrapper {

  def getDataFrame(): DataFrame = {
    spark.sparkContext.setLogLevel("ERROR")

    spark.read.format("csv")
      .option("header", "true")
      .load(
        "/home/mauricio/Documentos/spark/spark/2009.csv",
        "/home/mauricio/Documentos/spark/spark/2010.csv"
      )
  }

  def getAirlines(): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .load("/home/mauricio/Documentos/spark/spark/airlines.csv")
  }
}
