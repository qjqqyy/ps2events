package net.b0ss.ps2events

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import scopt.OParser

import java.time.LocalDate

object Main {
  final val PROGRAM_NAME = "ps2-events-streamer"

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Job]
    val parser = {
      import builder._
      val streamCommand = cmd("stream")
        .text("runs job which logs websocket events")
        .action((_, _) => LoggerJob())
        .children(
          opt[Int]("interval")
            .optional()
            .text("commit interval (in seconds) for Spark streaming, default: 60s")
            .action((x, j) => j.asInstanceOf[LoggerJob].copy(batchDuration = Seconds(x))),
          opt[String]("service-id")
            .required()
            .text("service ID for DayBreak API")
            .action((x, j) => j.asInstanceOf[LoggerJob].copy(serviceId = x)),
          arg[String]("<log location>")
            .required()
            .text("hadoop-compatible location to save data in")
            .action((x, j) => j.asInstanceOf[LoggerJob].copy(tableLocation = x)),
        )
      val compactionCommand = cmd("compact")
        .text("runs compaction job to convert full day of avro log data to parquet")
        .action((_, _) => CompactionJob())
        .children(
          opt[String]("date")
            .optional()
            .text("date of data to trigger compaction for, default: yesterday")
            .action((x, j) => j.asInstanceOf[CompactionJob].copy(date = LocalDate.parse(x))),
          arg[String]("<log location>")
            .required()
            .text("location that log files were written to by logger job")
            .action((x, j) => j.asInstanceOf[CompactionJob].copy(inputBasePath = x)),
          arg[String]("<output location>")
            .required()
            .text("base path of parquet files")
            .action((x, j) => j.asInstanceOf[CompactionJob].copy(outputBasePath = x)),
        )
      OParser.sequence(programName(PROGRAM_NAME), streamCommand, compactionCommand)
    }

    OParser.parse(parser, args, null).foreach { job =>
      val conf = job.setSparkConf(new SparkConf().setAppName(PROGRAM_NAME))
      val spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()

      job.run(spark)
    }
  }
}
