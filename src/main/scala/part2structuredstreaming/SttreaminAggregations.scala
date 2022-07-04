package part2structuredstreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

/**
  * @author nchakraborty
  */
object SttreaminAggregations {
    val spark = SparkSession.builder()
      .appName("StreamingAggregation")
      .master("local[2]")
      .getOrCreate()

    def streamingCount() = {
      val lines:DataFrame = spark.readStream
        .format("socket")
        .option("host","localhost").option("port","4321")
        .load()

      val lineCount = lines.selectExpr("count(1) as lineCount")

      lineCount.writeStream
        .format("console")
        .outputMode("complete") //Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;;
        .start().awaitTermination()
    }

  def numericalAggregations(aggFunction: Column => Column,name:String) = {
    println(s"name is $name")
    val lines:DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","4321")
      .load()

    val numbers :DataFrame = lines.select(col("value").cast("int").as("number"))
    val sum = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    sum.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }
  def groupNames() = {
    val lines:DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","4321")
      .load()

    val names = lines.select(col("value").as("name")).groupBy(col("name")).count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //numericalAggregations(count,"Niloy")
    groupNames()
  }
}
