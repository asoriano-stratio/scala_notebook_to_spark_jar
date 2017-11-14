package com.stratio.intelligence


import org.apache.spark.sql.SparkSession


object App extends App{

  val spark = SparkSession.builder
    .appName("test_app")
    .getOrCreate()

{{ custom_code }}


}
