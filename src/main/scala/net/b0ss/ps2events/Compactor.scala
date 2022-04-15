package net.b0ss.ps2events

import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql._

import java.time.LocalDate

class Compactor(spark: SparkSession, date: LocalDate) {
  import spark.implicits._

  private val startTs = date.toEpochDay * 86400

  def load(inPath: String): DataFrame = spark.read
    .format("avro")
    .load(inPath) // AWS globbing is slow, do 1 file listing then pushdown partition filters
    .filter(
      $"log_date" === date.toString ||
        ($"log_date" === date.plusDays(1).toString && $"log_hour" === "00")
    )

  def compacted(df: DataFrame): DataFrame = df
    .filter($"timestamp" >= startTs && $"timestamp" < startTs + 86400)
    .drop("log_date", "log_hour")
    .withColumn("date", from_unixtime($"timestamp", "yyyy-MM-dd"))
    .repartition($"world_id", $"date")
    .distinct()
    .sortWithinPartitions($"character_id", $"timestamp")

  def save(df: DataFrame, outPath: String): Unit = df.write
    .partitionBy("world_id", "date")
    .mode(SaveMode.Append)
    .parquet(outPath)

  def runCompaction(inPath: String, outPath: String): Unit = save(compacted(load(inPath)), outPath)

}
