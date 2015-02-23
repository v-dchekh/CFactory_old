package com.dchekh

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.UUID
import kafka.consumer._
import kafka.producer._
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericDatumReader, GenericData, GenericRecord, GenericDatumWriter }
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.DatumReader
import java.io.ByteArrayInputStream
import java.io.File
import scala.collection.mutable.HashMap

object AvroWrapper {
  final val MAGIC = Array[Byte](0x0)
  
  def decode(bytes: Array[Byte], schema: Schema): GenericRecord = {
    val stream = new ByteArrayInputStream(bytes)
    val avroDencoderFactory = DecoderFactory.get()
    val binaryDecoder = avroDencoderFactory.binaryDecoder(stream, null)
    val datumReader = new GenericDatumReader[GenericRecord](schema)
    var record = new GenericData.Record(schema)
    datumReader.read(record, binaryDecoder)
  }
  
  def decode(bytes: Array[Byte]): GenericRecord = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val magic = new Array[Byte](1)
    decoder.readFixed(magic)
    
    if (magic.deep != MAGIC.deep) throw new IllegalArgumentException("Not a camus byte array")

    val schemaIdArray = new Array[Byte](4)
    decoder.readFixed(schemaIdArray)

    val schemaOpt = CFactory.schema_list.get(ByteBuffer.wrap(schemaIdArray).getInt.toString())
    schemaOpt match {
      case None => throw new IllegalArgumentException("Invalid schema id")
      case Some(schema) =>
        val reader = new GenericDatumReader[GenericRecord](schema)
        reader.read(null, decoder)
    }
  }

}