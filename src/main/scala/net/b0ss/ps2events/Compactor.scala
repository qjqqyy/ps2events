package net.b0ss.ps2events

import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.{ SaveMode, SparkSession }

import java.time.LocalDate

class Compactor(spark: SparkSession, inPath: String, outPath: String) {
  import spark.implicits._

  def compact(date: LocalDate): Unit = {
    val startTs = date.toEpochDay * 86400
    spark.read
      .format("avro")
      .load(inPath) // AWS globbing is slow, do 1 file listing then pushdown partition filters
      .filter(
        $"log_date" === date.toString ||
          ($"log_date" === date.plusDays(1).toString && $"log_hour" === "00")
      )
      .filter($"timestamp" >= startTs && $"timestamp" < startTs + 86400)
      .drop("log_date", "log_hour")
      .withColumn("date", from_unixtime($"timestamp", "yyyy-MM-dd"))
      .repartition($"world_id", $"date")
      .distinct()
      .sortWithinPartitions($"character_id", $"timestamp")
      .write
      .partitionBy("world_id", "date")
      .mode(SaveMode.Append)
      .parquet(outPath)
  }
}
