package net.b0ss.ps2events

import net.b0ss.ps2events.Ps2EventStreamer._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.streaming.{ StreamingContext, Time }

import java.time.format.DateTimeFormatter
import java.time.{ Instant, ZoneId }
import java.util.UUID

class Ps2EventStreamer(spark: SparkSession, ssc: StreamingContext, serviceId: String) {
  import spark.implicits._

  private val ps2eventsStream = ssc.receiverStream(new Ps2WsReceiver(serviceId))

  private val uuid = UUID.randomUUID().toString
  private var batchId = 0

  private def getPartitionPathAndIncrementCounter(time: Time): String = {
    batchId += 1
    "%s/%s-%d".format(
      formatter.format(Instant.ofEpochMilli(time.milliseconds).atZone(ZoneId.systemDefault())),
      uuid,
      batchId,
    )
  }

  private val eventSchema: StructType = StructType(
    Seq(
      StructField("type", StringType),
      StructField("service", StringType),
      StructField(
        "payload",
        StructType(EVENT_PAYLOAD_COLUMNS.map { case (colName, _) => StructField(colName, StringType) }),
      ),
    )
  )

  private val selectCols =
    EVENT_PAYLOAD_COLUMNS.map { case (colName, colType) => $"payload.$colName".cast(colType).as(colName) }

  def save(basePath: String): Unit = {
    ps2eventsStream.foreachRDD { (eventsRdd, time) =>
      spark.read
        .schema(eventSchema)
        .json(eventsRdd.toDS())
        .filter($"type" === "serviceMessage" && $"service" === "event")
        .select(selectCols: _*)
        .coalesce(1)
        .write
        .format("avro")
        .mode(SaveMode.ErrorIfExists)
        .save(s"$basePath/${getPartitionPathAndIncrementCounter(time)}")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

object Ps2EventStreamer {
  private val formatter = DateTimeFormatter.ofPattern("'log_date='yyyy-MM-dd/'log_hour='HH")

  final val EVENT_PAYLOAD_COLUMNS: Vector[(String, DataType)] = Vector(
    ("achievement_id", IntegerType),
    ("amount", IntegerType),
    ("attacker_character_id", LongType),
    ("attacker_fire_mode_id", IntegerType),
    ("attacker_loadout_id", IntegerType),
    ("attacker_vehicle_id", IntegerType),
    ("attacker_weapon_id", IntegerType),
    ("battle_rank", IntegerType),
    ("character_id", LongType),
    ("character_loadout_id", IntegerType),
    ("context", StringType),
    ("duration_held", IntegerType),
    ("event_name", StringType),
    // ("event_type", StringType), // inaccurate docs?
    ("experience_bonus", FloatType),
    ("experience_id", IntegerType),
    ("facility_id", IntegerType),
    ("faction_id", IntegerType),
    ("faction_nc", FloatType),
    ("faction_tr", FloatType),
    ("faction_vs", FloatType),
    ("instance_id", IntegerType),
    ("is_headshot", BooleanType),
    ("item_count", IntegerType),
    ("item_id", IntegerType),
    ("loadout_id", IntegerType),
    ("metagame_event_id", IntegerType),
    ("metagame_event_state", IntegerType),
    ("metagame_event_state_name", StringType),
    ("nc_population", IntegerType),
    ("new_faction_id", IntegerType),
    ("old_faction_id", IntegerType),
    ("other_id", LongType),
    ("outfit_id", LongType),
    ("previous_faction", IntegerType),
    ("skill_id", IntegerType),
    ("timestamp", IntegerType),
    ("tr_population", IntegerType),
    ("triggering_faction", IntegerType),
    ("vehicle_id", IntegerType),
    ("vs_population", IntegerType),
    ("world_id", IntegerType),
    ("zone_id", IntegerType),
  )
}
