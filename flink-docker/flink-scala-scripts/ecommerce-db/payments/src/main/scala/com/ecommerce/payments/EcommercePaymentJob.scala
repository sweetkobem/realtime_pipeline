package com.ecommerce.payments

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

case class EcommercePayment(
    payment_id: Long,
    order_id: Long,
    amount: BigDecimal,
    payment_method: String,
    payment_date: String,
    status: String,
    created_at: String,
    updated_at: String
)

object EcommercePaymentStatementBuilder extends JdbcStatementBuilder[EcommercePayment] with Serializable {
  override def accept(stmt: PreparedStatement, data: EcommercePayment): Unit = {
    stmt.setLong(1, data.payment_id)
    stmt.setLong(2, data.order_id)
    stmt.setBigDecimal(3, data.amount.bigDecimal)
    stmt.setString(4, data.payment_method)

    val paymentDateTs = java.sql.Timestamp.from(Instant.parse(data.payment_date))
    stmt.setTimestamp(5, paymentDateTs)

    stmt.setString(6, data.status)

    val createdTs = java.sql.Timestamp.from(Instant.parse(data.created_at))
    val updatedTs = java.sql.Timestamp.from(Instant.parse(data.updated_at))
    stmt.setTimestamp(7, createdTs)
    stmt.setTimestamp(8, updatedTs)
  }
}

object EcommercePaymentJob {
  def main(args: Array[String]): Unit = {
    val GROUP_ID = "flink-ecommerce-db"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envVars = getEnvVars()

    val topic = "ecommerce_db.public.payments"
    val kafkaSource = buildKafkaSource[EcommercePayment](
      topic,
      envVars("SCHEMA_REGISTRY_SERVER"),
      envVars("KAFKA_SERVER"),
      GROUP_ID,
      record => EcommercePayment(
        payment_id = getLong(record, "payment_id"),
        order_id = getLong(record, "order_id"),
        amount = getBigDecimal(record, "amount", precision=12, scale=2),
        payment_method = getString(record, "payment_method"),
        payment_date = getRequiredTimestamp(record, "payment_date"),
        status = getString(record, "status"),
        created_at = getRequiredTimestamp(record, "created_at"),
        updated_at = getRequiredTimestamp(record, "updated_at")
      )
    )

    val jdbcSink = buildJdbcSink[EcommercePayment](
        """
            |INSERT INTO bronze_hot_ecommerce_payment
            |(payment_id, order_id, amount, payment_method, payment_date, status, created_at, updated_at)
            |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin,
        EcommercePaymentStatementBuilder,
        envVars("DATABASE_SERVER"),
        envVars("DATABASE_USERNAME"),
        envVars("DATABASE_PASSWORD")
    )

    val rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val inputStream = rawStream.filter(_ != null)

    inputStream.print("Raw Kafka Record:")

    val dedupedStream = inputStream
        .keyBy(_.payment_id)
        .process(new LastSeenDeduplicateFunction[EcommercePayment](_.payment_id))
    dedupedStream.print("After Deduplication:")
    
    dedupedStream.addSink(jdbcSink)

    env.execute("Ecommerce Payment")
  }
}
