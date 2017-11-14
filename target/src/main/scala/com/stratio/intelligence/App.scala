package com.stratio.intelligence


import org.apache.spark.sql.SparkSession


object App extends App{

  val spark = SparkSession.builder
    .appName("test_app")
    .getOrCreate()

val countDf = spark.range(10)
for( i <- countDf.collect() ){
    println(i)
}
import org.apache.spark.sql.functions.rand
val newDf = countDf.withColumn("rand", rand())
newDf.show()



}