package com.plugin

import java.time.Instant
import java.math.{BigDecimal => JBigDecimal}
import java.nio.ByteBuffer
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Conversions

object RecordUtils {
  private val decimalConversion = new Conversions.DecimalConversion()

  def getLong(record: org.apache.avro.generic.GenericRecord, field: String): Long = {
    record.get(field) match {
      case n: java.lang.Number => n.longValue()
      case _ => 0L
    }
  }

  def getDouble(record: org.apache.avro.generic.GenericRecord, field: String): Double = {
    Option(record.get(field)) match {
      case Some(n: java.lang.Number) => n.doubleValue()
      case Some(str: String) =>
        try str.toDouble catch { case _: Throwable => 0.0 }
      case Some(other) =>
        throw new RuntimeException(s"Unsupported type for field '$field': ${other.getClass} - value: $other")
      case None => 0.0
    }
  }

  def getBigDecimal(record: org.apache.avro.generic.GenericRecord, field: String, precision: Int, scale: Int): BigDecimal = {
    Option(record.get(field)) match {
      case Some(buf: ByteBuffer) =>
        val decimalType = LogicalTypes.decimal(precision, scale)
        val bytesSchema = Schema.create(Schema.Type.BYTES)
        decimalType.addToSchema(bytesSchema)
        val javaDecimal: JBigDecimal = decimalConversion.fromBytes(buf, bytesSchema, decimalType)
        BigDecimal(javaDecimal)
      case Some(n: java.lang.Number) =>
        BigDecimal(n.toString)
      case Some(other) =>
        throw new RuntimeException(s"Unsupported type for field '$field': ${other.getClass} - value: $other")
      case None =>
        BigDecimal(0)
    }
  }

  def getInt(record: org.apache.avro.generic.GenericRecord, field: String): Int = {
    record.get(field) match {
      case n: java.lang.Number => n.intValue()
      case _ => 0
    }
  }

  def getString(record: org.apache.avro.generic.GenericRecord, field: String): String = {
    Option(record.get(field)).map(_.toString).getOrElse("")
  }

  def getOptionalString(record: org.apache.avro.generic.GenericRecord, field: String): Option[String] = {
    Option(record.get(field)).map(_.toString)
  }

  def getOptionalLong(record: org.apache.avro.generic.GenericRecord, field: String): Option[Long] = {
    Option(record.get(field)) match {
      case Some(n: java.lang.Number) => Some(n.longValue())
      case _ => None
    }
  }

  def getOptionalTimestamp(record: org.apache.avro.generic.GenericRecord, field: String): String = {
    Option(record.get(field))
      .map(_.asInstanceOf[Long] / 1000)
      .map(Instant.ofEpochMilli)
      .map(_.toString)
      .orNull
  }

  def getRequiredTimestamp(record: org.apache.avro.generic.GenericRecord, field: String): String = {
    val ts = getLong(record, field)
    Instant.ofEpochMilli(ts / 1000).toString
  }
}