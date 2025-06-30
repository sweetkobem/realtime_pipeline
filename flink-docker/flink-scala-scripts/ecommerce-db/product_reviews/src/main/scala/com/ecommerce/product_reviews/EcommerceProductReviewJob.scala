package com.ecommerce.product_reviews

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

case class EcommerceProductReview(
    review_id: Long,
    product_id: Long,
    user_id: Long,
    rating: Int,
    comment: Option[String],
    created_at: String,
    updated_at: String
)

object EcommerceProductReviewStatementBuilder extends JdbcStatementBuilder[EcommerceProductReview] with Serializable {
  override def accept(stmt: PreparedStatement, data: EcommerceProductReview): Unit = {
    stmt.setLong(1, data.review_id)
    stmt.setLong(2, data.product_id)
    stmt.setLong(3, data.user_id)
    stmt.setInt(4, data.rating)
    stmt.setString(5, data.comment.orNull)

    val createdTs = java.sql.Timestamp.from(Instant.parse(data.created_at))
    val updatedTs = java.sql.Timestamp.from(Instant.parse(data.updated_at))
    stmt.setTimestamp(6, createdTs)
    stmt.setTimestamp(7, updatedTs)
  }
}

object EcommerceProductReviewJob {
  def main(args: Array[String]): Unit = {
    val GROUP_ID = "flink-ecommerce-db"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envVars = getEnvVars()

    val topic = "ecommerce_db.public.product_reviews"
    val kafkaSource = buildKafkaSource[EcommerceProductReview](
      topic,
      envVars("SCHEMA_REGISTRY_SERVER"),
      envVars("KAFKA_SERVER"),
      GROUP_ID,
      record => EcommerceProductReview(
        review_id = getLong(record, "review_id"),
        product_id = getLong(record, "product_id"),
        user_id = getLong(record, "user_id"),
        rating = getInt(record, "rating"),
        comment = getOptionalString(record, "comment"),
        created_at = getRequiredTimestamp(record, "created_at"),
        updated_at = getRequiredTimestamp(record, "updated_at")
      )
    )

    val jdbcSink = buildJdbcSink[EcommerceProductReview](
        """
            |INSERT INTO bronze_hot_ecommerce_product_review
            |(review_id, product_id, user_id, rating, comment, created_at, updated_at)
            |VALUES (?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin,
        EcommerceProductReviewStatementBuilder,
        envVars("DATABASE_SERVER"),
        envVars("DATABASE_USERNAME"),
        envVars("DATABASE_PASSWORD")
    )

    val rawStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val inputStream = rawStream.filter(_ != null)

    inputStream.print("Raw Kafka Record:")

    val dedupedStream = inputStream
        .keyBy(_.review_id)
        .process(new LastSeenDeduplicateFunction[EcommerceProductReview](_.review_id))
    dedupedStream.print("After Deduplication:")
    
    dedupedStream.addSink(jdbcSink)

    env.execute("Ecommerce Product Review")
  }
}
