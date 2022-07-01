package part2structuredstreaming

import common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt

/**
  * @author nchakraborty
  */
object StreamingDataFrames {
  val spark = SparkSession.builder()
    .appName("OurFirstStreams")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket() = {
    //reading from DF
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","4321")
      .load()
    // tranformation

    val shortLine:DataFrame = lines.filter(length(col("value"))<=5)

    // consuming DF
    val query = shortLine.writeStream
      .format("console")
      .outputMode("Append")
      .start()

    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF = spark.readStream
      .format("csv")
      .option("header","false")
      .option("dateFormat","MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .outputMode("Append")
      .format("console")
      .start()
      .awaitTermination()
  }


  def demoTriggers() = {
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","4321")
      .load()

    lines.writeStream
      .format("console")
      .outputMode("Append")
      .trigger(
        //Trigger.ProcessingTime(2.seconds)
      //Trigger.Once()
      Trigger.Continuous(2.seconds)
      )
      .start().awaitTermination()
  }
  def main(args: Array[String]): Unit = {

    demoTriggers()

  }

}
