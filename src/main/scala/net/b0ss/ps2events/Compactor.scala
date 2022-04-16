package net.b0ss.ps2events

import net.b0ss.ps2events.Compactor.DATA_COLUMNS
import net.b0ss.ps2events.Ps2EventStreamer._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.net.URI
import java.time.LocalDate

class Compactor(spark: SparkSession) {
  import spark.implicits._

  def loadAvro(basePath: String, date: LocalDate): DataFrame = {
    val startTs = date.toEpochDay * 86400

    // s3 file listing is very slow, we do a recursive file listing and give spark the full path
    val fs = FileSystem.get(new URI(basePath), spark.sparkContext.hadoopConfiguration)
    val paths = (fs.listFiles(new Path(s"$basePath/log_date=${date.toString}"), true).toVector ++
      fs.listFiles(new Path(s"$basePath/log_date=${date.plusDays(1).toString}/log_hour=00"), true).toVector)
      .map(_.getPath.toString)

    spark.read
      .format("avro")
      .option("basePath", basePath)
      .load(paths: _*)
      .drop("log_batch_id")
      .drop("log_date", "log_hour")
      .filter($"timestamp" >= startTs && $"timestamp" < startTs + 86400)
  }

  def compacted(df: DataFrame): DataFrame = df
    .groupBy(DATA_COLUMNS :+ $"log_source": _*)
    .count()
    .withColumnRenamed("count", "_repeated")
    .groupBy(DATA_COLUMNS: _*)
    .agg(max($"_repeated").as("_repeated"))
    .withColumn("date", from_unixtime($"timestamp", "yyyy-MM-dd"))
    .repartition($"world_id", $"date")
    .sortWithinPartitions($"world_id", $"date", $"character_id", $"timestamp")

  def save(df: DataFrame, outPath: String): Unit = df.write
    .partitionBy("world_id", "date")
    .mode(SaveMode.Append)
    .parquet(outPath)

  def run(date: LocalDate, inPath: String, outPath: String): Unit = save(compacted(loadAvro(inPath, date)), outPath)

}

object Compactor {
  final val DATA_COLUMNS = EVENT_PAYLOAD_COLUMNS.map(p => col(p._1))

  class WithBackfill(spark: SparkSession, backfillDatasetLocation: String) extends Compactor(spark) {

    import spark.implicits._
    val backfillDf: DataFrame = spark.read.parquet(backfillDatasetLocation)

    def loadBackfill(date: LocalDate): DataFrame = spark.read
      .schema(EVENT_SCHEMA)
      .json(backfillDf.filter($"date" === date.toString).select($"_raw".as[String]))
      .filter($"type" === "serviceMessage" && $"service" === "event")
      .select(DATA_COLUMNS_WITH_CAST: _*)
      .withColumn("log_source", typedlit[Option[String]](Some("backfill")))

    override def run(date: LocalDate, inPath: String, outPath: String): Unit =
      save(compacted(loadAvro(inPath, date).union(loadBackfill(date))), outPath)
  }
}
