package net.b0ss.ps2events

import net.b0ss.ps2events.Ps2EventStreamer._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.{ TBL_NAME, INSERT_PARALLELISM_VALUE }
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.streaming.StreamingContext

class Ps2EventStreamer(spark: SparkSession, ssc: StreamingContext, location: String, serviceId: String) {
  import spark.implicits._

  private val ps2eventsStream = ssc.receiverStream(new Ps2WsReceiver(serviceId))

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
    from_unixtime($"parsed.payload.timestamp", "yyyy-MM-dd").as("date") +:
      $"value".as("_raw") +:
      EVENT_PAYLOAD_COLUMNS.map { case (colName, colType) => $"parsed.payload.$colName".cast(colType) }

  def run(): Unit = {
    ps2eventsStream.foreachRDD { eventsRdd =>
      spark
        .createDataset(eventsRdd)
        .withColumn("parsed", from_json($"value", eventSchema))
        .filter($"parsed.type" === "serviceMessage" && $"parsed.service" === "event")
        .select(selectCols: _*)
        .write
        .format("hudi")
        .options(HUDI_OPTIONS)
        .mode(SaveMode.Append)
        .save(location)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

object Ps2EventStreamer {
  final val HUDI_OPTIONS = Map(
    TBL_NAME.key -> "ps2_events",
    OPERATION.key -> INSERT_OPERATION_OPT_VAL,
    INSERT_PARALLELISM_VALUE.key -> "8",
    PRECOMBINE_FIELD.key -> "timestamp",
    HIVE_STYLE_PARTITIONING.key -> "true",
    RECORDKEY_FIELD.key -> "_raw",
    PARTITIONPATH_FIELD.key -> "world_id,date",
    KEYGENERATOR_CLASS_NAME.key -> classOf[ComplexKeyGenerator].getCanonicalName,
  )

  final val EVENT_PAYLOAD_COLUMNS: Vector[(String, DataType)] = Vector(
    ("event_name", StringType),
    ("character_id", LongType),
    ("timestamp", IntegerType),
    ("world_id", IntegerType),
    ("zone_id", IntegerType),
    ("experience_id", IntegerType),
    ("loadout_id", IntegerType),
    ("other_id", LongType),
    ("achievement_id", IntegerType),
    ("amount", IntegerType),
    ("attacker_character_id", LongType),
    ("attacker_fire_mode_id", IntegerType),
    ("attacker_loadout_id", IntegerType),
    ("attacker_vehicle_id", IntegerType),
    ("attacker_weapon_id", IntegerType),
    ("battle_rank", IntegerType),
    ("character_loadout_id", IntegerType),
    ("context", StringType),
    ("duration_held", IntegerType),
    ("event_type", StringType),
    ("experience_bonus", FloatType),
    ("facility_id", IntegerType),
    ("faction_id", IntegerType),
    ("faction_nc", FloatType),
    ("faction_tr", FloatType),
    ("faction_vs", FloatType),
    ("is_headshot", BooleanType),
    ("item_count", IntegerType),
    ("item_id", IntegerType),
    ("metagame_event_id", IntegerType),
    ("metagame_event_state", IntegerType),
    ("nc_population", IntegerType),
    ("tr_population", IntegerType),
    ("vs_population", IntegerType),
    ("new_faction_id", IntegerType),
    ("old_faction_id", IntegerType),
    ("outfit_id", LongType),
    ("previous_faction", IntegerType),
    ("skill_id", IntegerType),
    ("triggering_faction", IntegerType),
    ("vehicle_id", IntegerType),
  )
}
