package com.ecommerce.categories

import java.time.Instant
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import com.plugin.KafkaToClickHouse.{buildKafkaSource, buildJdbcSink, LastSeenDeduplicateFunction, getEnvVars}
import com.plugin.RecordUtils._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.scala.createTypeInformation
import scala.reflect.ClassTag
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import java.sql.PreparedStatement

case class EcommerceCategory(
    category_id: Long,
    name: String,
    parent_id: Option[Long],
    created_at: String,
    updated_at: String,
    deleted_at: String
)

object EcommerceCategoryStatementBuilder extends JdbcStatementBuilder[EcommerceCategory] with Serializable {
  override def accept(stmt: PreparedStatement, data: EcommerceCategory): Unit = {
    stmt.setLong(1, data.category_id)
    stmt.setString(2, data.name)
    data.parent_id match {
      case Some(id) => stmt.setLong(3, id)
      case None => stmt.setNull(3, java.sql.Types.BIGINT)
    }
    val createdTs = java.sql.Timestamp.from(Instant.parse(data.created_at))
    val updatedTs = java.sql.Timestamp.from(Instant.parse(data.updated_at))
    val deletedTs: java.sql.Timestamp = Option(data.deleted_at)
        .filter(_.nonEmpty)
        .map(Instant.parse)
        .map(java.sql.Timestamp.from)
        .orNull
    stmt.setTimestamp(4, createdTs)
    stmt.setTimestamp(5, updatedTs)
    stmt.setTimestamp(6, deletedTs)
  }
}

object EcommerceCategoryJob {
  def main(args: Array[String]): Unit = {
    val GROUP_ID = "flink-ecommerce-db"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envVars = getEnvVars()

    val topic = "ecommerce_db.public.categories"
    val kafkaSource = buildKafkaSource[EcommerceCategory](
      topic,
      envVars("SCHEMA_REGISTRY_SERVER"),
      envVars("KAFKA_SERVER"),
      GROUP_ID,
      record => EcommerceCategory(
        category_id = getLong(record, "category_id"),
        name = getString(record, "name"),
        parent_id = getOptionalLong(record, "parent_id"),
        created_at = getRequiredTimestamp(record, "created_at"),
        updated_at = getRequiredTimestamp(record, "updated_at"),
        deleted_at = getOptionalTimestamp(record, "deleted_at")
      )
    )

    val jdbcSink = buildJdbcSink[EcommerceCategory](
        """
            |INSERT INTO bronze_hot_ecommerce_category
            |(category_id, name, parent_id, created_at, updated_at, deleted_at)
            |VALUES (?, ?, ?, ?, ?, ?)
            |""".stripMargin,
        EcommerceCategoryStatementBuilder,
        envVars("DATABASE_SERVER"),
        envVars("DATABASE_USERNAME"),
        envVars("DATABASE_PASSWORD")
    )

    val rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val inputStream = rawStream.filter(_ != null)

    inputStream.print("Raw Kafka Record:")

    val dedupedStream = inputStream
        .keyBy(_.category_id)
        .process(new LastSeenDeduplicateFunction[EcommerceCategory](_.category_id))
    dedupedStream.print("After Deduplication:")
    
    dedupedStream.addSink(jdbcSink)

    env.execute("Ecommerce Categories")
  }
}
