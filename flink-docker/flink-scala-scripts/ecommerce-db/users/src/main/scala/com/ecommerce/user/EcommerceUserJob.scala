package com.ecommerce.users

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

case class EcommerceUser(
    user_id: Long,
    name: String,
    email: String,
    password_hash: String,
    phone: Option[String],
    created_at: String,
    updated_at: String,
    deleted_at: String
)

object EcommerceUserStatementBuilder extends JdbcStatementBuilder[EcommerceUser] with Serializable {
  override def accept(stmt: PreparedStatement, data: EcommerceUser): Unit = {
    stmt.setLong(1, data.user_id)
    stmt.setString(2, data.name)
    stmt.setString(3, data.email)
    stmt.setString(4, data.password_hash)
    stmt.setString(5, data.phone.orNull)
    val createdTs = java.sql.Timestamp.from(Instant.parse(data.created_at))
    val updatedTs = java.sql.Timestamp.from(Instant.parse(data.updated_at))
    val deletedTs: java.sql.Timestamp = Option(data.deleted_at)
        .filter(_.nonEmpty)
        .map(Instant.parse)
        .map(java.sql.Timestamp.from)
        .orNull
    stmt.setTimestamp(6, createdTs)
    stmt.setTimestamp(7, updatedTs)
    stmt.setTimestamp(8, deletedTs)
  }
}

object EcommerceUserJob {
  def main(args: Array[String]): Unit = {
    val GROUP_ID = "flink-ecommerce-db"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envVars = getEnvVars()

    val topic = "ecommerce_db.public.users"
    val kafkaSource = buildKafkaSource[EcommerceUser](
      topic,
      envVars("SCHEMA_REGISTRY_SERVER"),
      envVars("KAFKA_SERVER"),
      GROUP_ID,
      record => EcommerceUser(
        user_id = getLong(record, "user_id"),
        name = getString(record, "name"),
        email = getString(record, "email"),
        password_hash = getString(record, "password_hash"),
        phone = getOptionalString(record, "phone"),
        created_at = getRequiredTimestamp(record, "created_at"),
        updated_at = getRequiredTimestamp(record, "updated_at"),
        deleted_at = getOptionalTimestamp(record, "deleted_at")
      )
    )

    val jdbcSink = buildJdbcSink[EcommerceUser](
        """
            |INSERT INTO bronze_hot_ecommerce_user
            |(user_id, name, email, password_hash, phone, created_at, updated_at, deleted_at)
            |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin,
        EcommerceUserStatementBuilder,
        envVars("DATABASE_SERVER"),
        envVars("DATABASE_USERNAME"),
        envVars("DATABASE_PASSWORD")
    )

    val rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val inputStream = rawStream.filter(_ != null)

    inputStream.print("Raw Kafka Record:")

    val dedupedStream = inputStream
        .keyBy(_.user_id)
        .process(new LastSeenDeduplicateFunction[EcommerceUser](_.user_id))
    dedupedStream.print("After Deduplication:")
    
    dedupedStream.addSink(jdbcSink)

    env.execute("Ecommerce User")
  }
}
