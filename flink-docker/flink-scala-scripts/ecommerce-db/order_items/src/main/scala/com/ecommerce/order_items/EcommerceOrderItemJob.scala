package com.ecommerce.order_items

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

case class EcommerceOrderItem(
    order_item_id: Long,
    order_id: Long,
    product_id: Long,
    quantity: Int,
    price: BigDecimal,
    created_at: String,
    updated_at: String
)

object EcommerceOrderItemStatementBuilder extends JdbcStatementBuilder[EcommerceOrderItem] with Serializable {
  override def accept(stmt: PreparedStatement, data: EcommerceOrderItem): Unit = {
    stmt.setLong(1, data.order_item_id)
    stmt.setLong(2, data.order_id)
    stmt.setLong(3, data.product_id)
    stmt.setInt(4, data.quantity)
    stmt.setBigDecimal(5, data.price.bigDecimal)
    val createdTs = java.sql.Timestamp.from(Instant.parse(data.created_at))
    val updatedTs = java.sql.Timestamp.from(Instant.parse(data.updated_at))
    stmt.setTimestamp(6, createdTs)
    stmt.setTimestamp(7, updatedTs)
  }
}

object EcommerceOrderItemJob {
  def main(args: Array[String]): Unit = {
    val GROUP_ID = "flink-ecommerce-db"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envVars = getEnvVars()

    val topic = "ecommerce_db.public.order_items"
    val kafkaSource = buildKafkaSource[EcommerceOrderItem](
      topic,
      envVars("SCHEMA_REGISTRY_SERVER"),
      envVars("KAFKA_SERVER"),
      GROUP_ID,
      record => EcommerceOrderItem(
        order_item_id = getLong(record, "order_item_id"),
        order_id = getLong(record, "order_id"),
        product_id = getLong(record, "product_id"),
        quantity = getInt(record, "quantity"),
        price = getBigDecimal(record, "price", precision=12, scale=2),
        created_at = getRequiredTimestamp(record, "created_at"),
        updated_at = getRequiredTimestamp(record, "updated_at")
      )
    )

    val jdbcSink = buildJdbcSink[EcommerceOrderItem](
        """
            |INSERT INTO bronze_hot_ecommerce_order_item
            |(order_item_id, order_id, product_id, quantity, price, created_at, updated_at)
            |VALUES (?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin,
        EcommerceOrderItemStatementBuilder,
        envVars("DATABASE_SERVER"),
        envVars("DATABASE_USERNAME"),
        envVars("DATABASE_PASSWORD")
    )

    val rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val inputStream = rawStream.filter(_ != null)

    inputStream.print("Raw Kafka Record:")

    val dedupedStream = inputStream
        .keyBy(_.order_item_id)
        .process(new LastSeenDeduplicateFunction[EcommerceOrderItem](_.order_item_id))
    dedupedStream.print("After Deduplication:")
    
    dedupedStream.addSink(jdbcSink)

    env.execute("Ecommerce Order Item")
  }
}
