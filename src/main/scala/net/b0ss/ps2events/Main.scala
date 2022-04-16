package net.b0ss.ps2events

import org.apache.spark.streaming.Seconds
import scopt.OParser

import java.time.LocalDate

object Main {
  final val PROGRAM_NAME = "ps2-events"

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Job]
    implicit val localDateRead: scopt.Read[LocalDate] = scopt.Read.reads(LocalDate.parse(_))

    val parser = {
      import builder._
      val streamCommand = cmd("stream")
        .text("runs job which logs websocket events")
        .action((_, _) => LoggerJob())
        .children(
          opt[Int]("interval")
            .optional()
            .valueName("<seconds>")
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
          opt[LocalDate]("date")
            .minOccurs(0)
            .maxOccurs(1000)
            .valueName("<yyyy-mm-dd>")
            .text("date(s) of data to trigger compaction for, default: yesterday, can specify multiple times")
            .action((x, j) => j.asInstanceOf[CompactionJob].copy(dates = x :: j.asInstanceOf[CompactionJob].dates)),
          opt[String]("backfill")
            .optional()
            .valueName("<location>")
            .text("location for backfill data (format: parquet, schema:<_raw:STRING, date:STRING>)")
            .action((x, j) => j.asInstanceOf[CompactionJob].copy(backfillLocation = Some(x))),
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

    OParser.parse(parser, args, null).foreach(_.run())
  }
}
