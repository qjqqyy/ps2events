package net.b0ss.ps2events

import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import scopt.OParser

object Main {
  final val PROGRAM_NAME = "ps2-events-streamer"

  case class Config(
      batchDuration: Duration = Seconds(60),
      tableLocation: String = "",
      serviceId: String = "",
  )

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName(PROGRAM_NAME),
        opt[Int]("interval")
          .optional()
          .text("commit interval (in seconds) for Spark streaming, default: 60s")
          .action((x, c) => c.copy(batchDuration = Seconds(x))),
        opt[String]("service-id")
          .required()
          .text("service ID for DayBreak API")
          .action((x, c) => c.copy(serviceId = x)),
        arg[String]("<location>")
          .required()
          .text("hadoop-compatible location to save data in")
          .action((x, c) => c.copy(tableLocation = x)),
      )
    }

    OParser.parse(parser, args, Config()) match {
      case None => ()
      case Some(config) =>
        val conf = SparkRDDWriteClient.registerClasses(new SparkConf().setAppName(PROGRAM_NAME))
        val spark = SparkSession
          .builder()
          .config(conf)
          .getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, config.batchDuration)

        new Ps2EventStreamer(spark, ssc, config.tableLocation, config.serviceId).run()
    }
  }
}
