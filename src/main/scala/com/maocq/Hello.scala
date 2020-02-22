package com.maocq

object Hello extends App with SparkSessionWrapper {

  spark.sparkContext.setLogLevel("ERROR")

  val dataFrame = spark.read.csv("/home/mauricio/Documentos/spark/sql/FL_insurance_sample.csv")
  dataFrame.printSchema()
}
