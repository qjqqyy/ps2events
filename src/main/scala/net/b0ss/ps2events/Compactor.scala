package net.b0ss.ps2events

import net.b0ss.ps2events.Ps2EventStreamer.EVENT_PAYLOAD_COLUMNS
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{ col, from_unixtime }

import java.time.LocalDate

class Compactor(spark: SparkSession) {
  import spark.implicits._

  def loadAvro(inPath: String): DataFrame = spark.read
    .format("avro")
    .load(inPath) // AWS globbing is slow, do 1 file listing then pushdown partition filters

  def filteredByDate(df: DataFrame, date: LocalDate): DataFrame = {
    val startTs = date.toEpochDay * 86400
    df.filter(
      $"log_date" === date.toString ||
        ($"log_date" === date.plusDays(1).toString && $"log_hour" === "00")
    ).filter($"timestamp" >= startTs && $"timestamp" < startTs + 86400)
      .drop("log_date", "log_hour")
  }

  def compacted(df: DataFrame): DataFrame = df
    .groupBy(EVENT_PAYLOAD_COLUMNS.map(c => col(c._1)): _*)
    .count
    .withColumnRenamed("count", "_event_multiplicity")
    .withColumn("date", from_unixtime($"timestamp", "yyyy-MM-dd"))
    .repartition($"world_id", $"date")
    .distinct()
    .sortWithinPartitions($"world_id", $"date", $"character_id", $"timestamp")

  def save(df: DataFrame, outPath: String): Unit = df.write
    .partitionBy("world_id", "date")
    .mode(SaveMode.Append)
    .parquet(outPath)

  def run(date: LocalDate, inPath: String, outPath: String): Unit = Some(inPath)
    .map(loadAvro)
    .map(filteredByDate(_, date))
    .map(compacted)
    .foreach(save(_, outPath))

}

object Compactor {
  final val DATA_COLUMNS = EVENT_PAYLOAD_COLUMNS.map(_._1)
}
