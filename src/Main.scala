import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

import java.sql.Timestamp

object Main extends App {
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  val spark = SparkSession.builder
    .appName("StructuredStreamingBoilerplate")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .option("includeTimestamp", value = true)
    .load()

  val words = lines
    .as[(String, Timestamp)]
    .flatMap(line => line._1.split(" ").map(word => (word, line._2)))
    .toDF("word", "timestamp")

  val windowedCounts = words
    .groupBy(
      window($"timestamp", "10 seconds", "5 seconds"),
      $"word"
    )
    .count()
    .orderBy("window")

  val query = windowedCounts.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()

  query.awaitTermination()
}
