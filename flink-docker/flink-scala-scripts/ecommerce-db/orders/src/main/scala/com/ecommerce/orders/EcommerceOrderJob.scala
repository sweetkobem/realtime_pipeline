package com.ecommerce.orders

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

case class EcommerceOrder(
    order_id: Long,
    user_id: Long,
    order_status: String,
    total_amount: BigDecimal,
    created_at: String,
    updated_at: String
)

object EcommerceOrderStatementBuilder extends JdbcStatementBuilder[EcommerceOrder] with Serializable {
  override def accept(stmt: PreparedStatement, data: EcommerceOrder): Unit = {
    stmt.setLong(1, data.order_id)
    stmt.setLong(2, data.user_id)
    stmt.setString(3, data.order_status)
    stmt.setBigDecimal(4, data.total_amount.bigDecimal)
    val createdTs = java.sql.Timestamp.from(Instant.parse(data.created_at))
    val updatedTs = java.sql.Timestamp.from(Instant.parse(data.updated_at))
    stmt.setTimestamp(5, createdTs)
    stmt.setTimestamp(6, updatedTs)
  }
}

object EcommerceOrderJob {
  def main(args: Array[String]): Unit = {
    val GROUP_ID = "flink-ecommerce-db"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envVars = getEnvVars()

    val topic = "ecommerce_db.public.orders"
    val kafkaSource = buildKafkaSource[EcommerceOrder](
      topic,
      envVars("SCHEMA_REGISTRY_SERVER"),
      envVars("KAFKA_SERVER"),
      GROUP_ID,
      record => EcommerceOrder(
        order_id = getLong(record, "order_id"),
        user_id = getLong(record, "user_id"),
        order_status = getString(record, "order_status"),
        total_amount = getBigDecimal(record, "total_amount", precision=12, scale=2),
        created_at = getRequiredTimestamp(record, "created_at"),
        updated_at = getRequiredTimestamp(record, "updated_at")
      )
    )

    val jdbcSink = buildJdbcSink[EcommerceOrder](
        """
            |INSERT INTO bronze_hot_ecommerce_order
            |(order_id, user_id, order_status, total_amount, created_at, updated_at)
            |VALUES (?, ?, ?, ?, ?, ?)
            |""".stripMargin,
        EcommerceOrderStatementBuilder,
        envVars("DATABASE_SERVER"),
        envVars("DATABASE_USERNAME"),
        envVars("DATABASE_PASSWORD")
    )

    val rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val inputStream = rawStream.filter(_ != null)

    inputStream.print("Raw Kafka Record:")

    val dedupedStream = inputStream
        .keyBy(_.order_id)
        .process(new LastSeenDeduplicateFunction[EcommerceOrder](_.order_id))
    dedupedStream.print("After Deduplication:")
    
    dedupedStream.addSink(jdbcSink)

    env.execute("Ecommerce Order")
  }
}
