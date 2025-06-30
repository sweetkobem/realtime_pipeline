package com.ecommerce.products

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

case class EcommerceProduct(
    product_id: Long,
    name: String,
    description: String,
    price: BigDecimal,
    stock_quantity: Int,
    category_id: Long,
    created_at: String,
    updated_at: String,
    deleted_at: String
)

object EcommerceProductStatementBuilder extends JdbcStatementBuilder[EcommerceProduct] with Serializable {
  override def accept(stmt: PreparedStatement, data: EcommerceProduct): Unit = {
    stmt.setLong(1, data.product_id)
    stmt.setString(2, data.name)
    stmt.setString(3, data.description)
    stmt.setBigDecimal(4, data.price.bigDecimal)
    stmt.setInt(5, data.stock_quantity)
    stmt.setLong(6, data.category_id)
    val createdTs = java.sql.Timestamp.from(Instant.parse(data.created_at))
    val updatedTs = java.sql.Timestamp.from(Instant.parse(data.updated_at))
    val deletedTs: java.sql.Timestamp = Option(data.deleted_at)
        .filter(_.nonEmpty)
        .map(Instant.parse)
        .map(java.sql.Timestamp.from)
        .orNull
    stmt.setTimestamp(7, createdTs)
    stmt.setTimestamp(8, updatedTs)
    stmt.setTimestamp(9, deletedTs)
  }
}

object EcommerceProductJob {
  def main(args: Array[String]): Unit = {
    val GROUP_ID = "flink-ecommerce-db"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envVars = getEnvVars()

    val topic = "ecommerce_db.public.products"
    val kafkaSource = buildKafkaSource[EcommerceProduct](
      topic,
      envVars("SCHEMA_REGISTRY_SERVER"),
      envVars("KAFKA_SERVER"),
      GROUP_ID,
      record => EcommerceProduct(
        product_id = getLong(record, "product_id"),
        name = getString(record, "name"),
        description = getString(record, "description"),
        price = getBigDecimal(record, "price", precision=12, scale=2),
        stock_quantity = getInt(record, "stock_quantity"),
        category_id = getLong(record, "category_id"),
        created_at = getRequiredTimestamp(record, "created_at"),
        updated_at = getRequiredTimestamp(record, "updated_at"),
        deleted_at = getOptionalTimestamp(record, "deleted_at")
      )
    )

    val jdbcSink = buildJdbcSink[EcommerceProduct](
        """
            |INSERT INTO bronze_hot_ecommerce_product
            |(product_id, name, description, price, stock_quantity, category_id, created_at, updated_at, deleted_at)
            |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin,
        EcommerceProductStatementBuilder,
        envVars("DATABASE_SERVER"),
        envVars("DATABASE_USERNAME"),
        envVars("DATABASE_PASSWORD")
    )

    val rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val inputStream = rawStream.filter(_ != null)

    inputStream.print("Raw Kafka Record:")

    val dedupedStream = inputStream
        .keyBy(_.product_id)
        .process(new LastSeenDeduplicateFunction[EcommerceProduct](_.product_id))
    dedupedStream.print("After Deduplication:")
    
    dedupedStream.addSink(jdbcSink)

    env.execute("Ecommerce Product")
  }
}
