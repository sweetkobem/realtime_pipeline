package com.plugin

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcExecutionOptions, JdbcConnectionOptions, JdbcStatementBuilder}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.sql.{PreparedStatement, Timestamp}
import java.time.Instant
import java.util.Properties
import scala.collection.JavaConverters._
import java.io.IOException
import scala.reflect.ClassTag

object KafkaToClickHouse {

    def getEnvVars(): Map[String, String] = {
        val keys = List("SCHEMA_REGISTRY_SERVER", "KAFKA_SERVER", "DATABASE_SERVER", "DATABASE_USERNAME", "DATABASE_PASSWORD")
        keys.map(k => k -> sys.env.getOrElse(k, throw new RuntimeException(s"$k not set"))).toMap
    }

    class GenericAvroDeserializer[T](schemaRegistryUrl: String, topic: String, convert: GenericRecord => T)(implicit typeInfo: TypeInformation[T], ct: ClassTag[T])
        extends AbstractDeserializationSchema[T](ct.runtimeClass.asInstanceOf[Class[T]]) {

        @transient lazy val deserializer: KafkaAvroDeserializer = {
            val props = Map(
                "schema.registry.url" -> schemaRegistryUrl,
                "specific.avro.reader" -> "false"
            ).asJava
            val d = new KafkaAvroDeserializer()
            d.configure(props, false)
            d
        }

        override def deserialize(message: Array[Byte]): T = {
            val record = deserializer.deserialize(topic, message).asInstanceOf[GenericRecord]
            val afterOpt = Option(record.get("after").asInstanceOf[GenericRecord])
            afterOpt match {
                case Some(after) => convert(after)
                case None        => null.asInstanceOf[T]
            }
        }

        override def getProducedType: TypeInformation[T] = typeInfo
    }

    class LastSeenDeduplicateFunction[T : TypeInformation](keySelector: T => Long)
        extends KeyedProcessFunction[Long, T, T] {

        lazy val state: ValueState[T] = getRuntimeContext.getState(
            new ValueStateDescriptor[T]("dedup-state", implicitly[TypeInformation[T]])
        )

        override def processElement(value: T, ctx: KeyedProcessFunction[Long, T, T]#Context, out: Collector[T]): Unit = {
            if (value != null) {
                state.update(value)
                out.collect(value)
            }
        }
    }


    def buildKafkaSource[T](
        topic: String,
        schemaRegistryUrl: String,
        kafkaServer: String,
        groupId: String,
        convert: GenericRecord => T
        )(implicit typeInfo: TypeInformation[T], ct: ClassTag[T]) = {
        KafkaSource.builder[T]()
            .setBootstrapServers(kafkaServer)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new GenericAvroDeserializer[T](schemaRegistryUrl, topic, convert))
            .build()
    }

  def buildJdbcSink[T](insertSql: String, jdbcBuilder: JdbcStatementBuilder[T], dbUrl: String, username: String, password: String) =
    JdbcSink.sink[T](
      insertSql,
      jdbcBuilder,
      JdbcExecutionOptions.builder().withBatchSize(1000).build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl(dbUrl)
        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
        .withUsername(username)
        .withPassword(password)
        .build()
    )
}
