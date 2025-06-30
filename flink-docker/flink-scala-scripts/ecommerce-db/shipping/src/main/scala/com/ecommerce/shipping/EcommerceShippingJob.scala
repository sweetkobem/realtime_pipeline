package com.ecommerce.shipping

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

case class EcommerceShipping(
    shipping_id: Long,
    order_id: Long,
    address: String,
    city: String,
    postal_code: String,
    country: String,
    shipping_date: String,
    tracking_number: Option[String],
    created_at: String,
    updated_at: String
)

object EcommerceShippingStatementBuilder extends JdbcStatementBuilder[EcommerceShipping] with Serializable {
  override def accept(stmt: PreparedStatement, data: EcommerceShipping): Unit = {
    stmt.setLong(1, data.shipping_id)
    stmt.setLong(2, data.order_id)
    stmt.setString(3, data.address)
    stmt.setString(4, data.city)
    stmt.setString(5, data.postal_code)
    stmt.setString(6, data.country)

    val shippingDateTs = java.sql.Timestamp.from(Instant.parse(data.shipping_date))
    stmt.setTimestamp(7, shippingDateTs)

    stmt.setString(8, data.tracking_number.orNull)

    val createdTs = java.sql.Timestamp.from(Instant.parse(data.created_at))
    val updatedTs = java.sql.Timestamp.from(Instant.parse(data.updated_at))
    stmt.setTimestamp(9, createdTs)
    stmt.setTimestamp(10, updatedTs)
  }
}

object EcommerceShippingJob {
  def main(args: Array[String]): Unit = {
    val GROUP_ID = "flink-ecommerce-db"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envVars = getEnvVars()

    val topic = "ecommerce_db.public.shipping"
    val kafkaSource = buildKafkaSource[EcommerceShipping](
      topic,
      envVars("SCHEMA_REGISTRY_SERVER"),
      envVars("KAFKA_SERVER"),
      GROUP_ID,
      record => EcommerceShipping(
        shipping_id = getLong(record, "shipping_id"),
        order_id = getLong(record, "order_id"),
        address = getString(record, "address"),
        city = getString(record, "city"),
        postal_code = getString(record, "postal_code"),
        country = getString(record, "country"),
        shipping_date = getRequiredTimestamp(record, "shipping_date"),
        tracking_number = getOptionalString(record, "tracking_number"),
        created_at = getRequiredTimestamp(record, "created_at"),
        updated_at = getRequiredTimestamp(record, "updated_at")
      )
    )

    val jdbcSink = buildJdbcSink[EcommerceShipping](
        """
            |INSERT INTO bronze_hot_ecommerce_shipping
            |(shipping_id, order_id, address, city, postal_code, country, shipping_date, tracking_number, created_at, updated_at)
            |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin,
        EcommerceShippingStatementBuilder,
        envVars("DATABASE_SERVER"),
        envVars("DATABASE_USERNAME"),
        envVars("DATABASE_PASSWORD")
    )

    val rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val inputStream = rawStream.filter(_ != null)

    inputStream.print("Raw Kafka Record:")

    val dedupedStream = inputStream
        .keyBy(_.shipping_id)
        .process(new LastSeenDeduplicateFunction[EcommerceShipping](_.shipping_id))
    dedupedStream.print("After Deduplication:")
    
    dedupedStream.addSink(jdbcSink)

    env.execute("Ecommerce Shipping")
  }
}
