package com.maocq.semana1

import com.maocq.SparkSessionWrapper
import org.apache.spark.sql.DataFrame

trait Data extends SparkSessionWrapper {

  def getDataFrame(): DataFrame = {
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIBXSZZ5DF56OT5WQ")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark.read
      .format("csv")
      .option("header", "true")
      .load("s3a://deutsche-boerse-xetra-pds/2020-02-28/*")
  }
}
