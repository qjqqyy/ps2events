package net.b0ss.ps2events

import net.b0ss.ps2events.Job.{ SPARK_ON_CLOUD_READER_CONF, SPARK_ON_CLOUD_WRITER_CONF }
import org.apache.spark.SparkConf
import org.apache.spark.internal.io.cloud.{ BindingParquetOutputCommitter, PathOutputCommitProtocol }
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

import java.time.LocalDate

sealed trait Job {
  def setSparkConf(conf: SparkConf): SparkConf
  def run(spark: SparkSession): Unit
}

object Job {
  final val SPARK_ON_CLOUD_WRITER_CONF = Map(
    // "spark.hadoop.fs.s3a.committer.name" -> "directory",
    // "spark.hadoop.fs.s3a.committer.name" -> "partitioned",
    "spark.hadoop.fs.s3a.committer.name" -> "magic",
    "spark.hadoop.parquet.enable.summary-metadata" -> "false",
    "spark.sql.parquet.output.committer.class" -> classOf[BindingParquetOutputCommitter].getCanonicalName,
    "spark.sql.sources.commitProtocolClass" -> classOf[PathOutputCommitProtocol].getCanonicalName,
  )
  final val SPARK_ON_CLOUD_READER_CONF = Map(
    // "spark.sql.hive.metastorePartitionPruning" -> "true", // not using hive
    "spark.sql.parquet.filterPushdown" -> "true",
    "spark.sql.parquet.mergeSchema" -> "false",
  )
}

case class LoggerJob(
    batchDuration: Duration = Seconds(60),
    serviceId: String = "",
    tableLocation: String = "",
) extends Job {

  def setSparkConf(conf: SparkConf): SparkConf = {
    if (tableLocation.startsWith("s3a://")) {
      SPARK_ON_CLOUD_WRITER_CONF.foreach { case (k, v) => conf.setIfMissing(k, v) }
    }
    conf
  }

  def run(spark: SparkSession): Unit =
    new Ps2EventStreamer(spark, new StreamingContext(spark.sparkContext, batchDuration), serviceId).save(tableLocation)
}

case class CompactionJob(
    inputBasePath: String = "",
    outputBasePath: String = "",
    date: LocalDate = LocalDate.now().minusDays(1),
) extends Job {

  def setSparkConf(conf: SparkConf): SparkConf = {
    if (inputBasePath.startsWith("s3a://")) {
      SPARK_ON_CLOUD_READER_CONF.foreach { case (k, v) => conf.setIfMissing(k, v) }
    }
    if (outputBasePath.startsWith("s3a://")) {
      SPARK_ON_CLOUD_WRITER_CONF.foreach { case (k, v) => conf.setIfMissing(k, v) }
    }
    conf
  }

  def run(spark: SparkSession): Unit =
    new Compactor(spark, inputBasePath, outputBasePath).compact(date)
}
