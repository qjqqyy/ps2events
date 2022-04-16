package net.b0ss.ps2events

import net.b0ss.ps2events.Job.{ SPARK_ON_CLOUD_READER_CONF, SPARK_ON_CLOUD_WRITER_CONF }
import org.apache.spark.SparkConf
import org.apache.spark.internal.io.cloud.{ BindingParquetOutputCommitter, PathOutputCommitProtocol }
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

import java.time.LocalDate

sealed trait Job {
  protected def sparkConf: SparkConf
  def run(): Unit

  final protected lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
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

  lazy val sparkConf: SparkConf = {
    val conf = new SparkConf().setAppName("ps2-events-logger")
    if (tableLocation.startsWith("s3a://")) SPARK_ON_CLOUD_WRITER_CONF.foreach((conf.setIfMissing _).tupled)
    conf
  }

  lazy val ssc = new StreamingContext(spark.sparkContext, batchDuration)

  def run(): Unit = new Ps2EventStreamer(spark, ssc, serviceId).save(tableLocation)
}

case class CompactionJob(
    inputBasePath: String = "",
    outputBasePath: String = "",
    dates: List[LocalDate] = Nil,
    backfillLocation: Option[String] = None,
) extends Job {
  private lazy val datesToRun = if (dates.nonEmpty) dates.toSet else Set(LocalDate.now().minusDays(1))

  lazy val sparkConf: SparkConf = {
    val conf = new SparkConf().setAppName("ps2-events-compactor")
    if (inputBasePath.startsWith("s3a://")) SPARK_ON_CLOUD_READER_CONF.foreach((conf.setIfMissing _).tupled)
    if (outputBasePath.startsWith("s3a://")) SPARK_ON_CLOUD_WRITER_CONF.foreach((conf.setIfMissing _).tupled)
    conf
  }

  def run(): Unit = {
    val compactor = backfillLocation match {
      case Some(location) => new Compactor.WithBackfill(spark, location)
      case None           => new Compactor(spark)
    }

    datesToRun.foreach(compactor.run(_, inputBasePath, outputBasePath))
  }
}
