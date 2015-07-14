package org.apache.streams.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.streams.core.{StreamsPersistWriter, StreamsDatum, StreamsProcessor}
import org.apache.streams.jackson.StreamsJacksonMapper
import org.apache.streams.pojo.json.{ActivityObject, Activity}
import org.apache.streams.util.SerializationUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object StreamsSparkBuilder {

  private val LOGGER: Logger = LoggerFactory.getLogger("StreamsSparkHelper")

  def asDatum(in: Object) : Option[StreamsDatum] = {
    val out = Try(new StreamsDatum(in))
    out match {
      case Success(v : StreamsDatum) =>
        if( out != null ) return Some(v) else return None
      case Failure(e : Throwable) =>
        LOGGER.warn(in.toString)
        return None
      case _ =>
        return None
    }
  }

  def asDatum(iter: Iterator[Object]) : Iterator[StreamsDatum] = {
    iter.flatMap(item => asDatum(item))
  }

  def applyProcessor(in: StreamsDatum, processor: StreamsProcessor) : Option[Iterator[StreamsDatum]] = {
    val out = Try(processor.process(in))
    out match {
      case Success(v : java.util.List[StreamsDatum]) =>
        if( out != null && v.size > 0 )
          return Some(v.iterator)
        else return None
      case Failure(e : Throwable) =>
        LOGGER.warn(in.getId, e)
        return None
      case _ =>
        return None
    }
  }

  def datumDocAsString(in: StreamsDatum, mapper: ObjectMapper) : Option[String] = {
    val mapper = StreamsJacksonMapper.getInstance()
    val out = Try(in.getDocument)
    out match {
      case Success(v : String) =>
        if( out != null ) return Some(v) else return None
      case Success(v : Activity) =>
        if( out != null ) return Some(mapper.writeValueAsString(v)) else return None
      case Success(v : ActivityObject) =>
        if( out != null ) return Some(mapper.writeValueAsString(v)) else return None
      case Failure(e : Throwable) =>
        LOGGER.warn(in.getId, e)
        return None
      case _ =>
        return None
    }
  }

  def datumDocAsString(iter: Iterator[StreamsDatum]) : Iterator[String] = {
    val mapper = StreamsJacksonMapper.getInstance()
    iter.flatMap(item => datumDocAsString(item, mapper))
  }

  def toJson(in: Object, mapper: ObjectMapper) : Option[String] = {
    val mapper = StreamsJacksonMapper.getInstance()
    val out = Try(in)
    out match {
      case Success(v : String) =>
        if( out != null ) return Some(v) else return None
      case Success(v : Activity) =>
        if( out != null ) return Some(mapper.writeValueAsString(v)) else return None
      case Success(v : ActivityObject) =>
        if( out != null ) return Some(mapper.writeValueAsString(v)) else return None
      case Failure(e : Throwable) =>
        LOGGER.warn(in.toString, e)
        return None
      case _ =>
        return None
    }
  }

  def mapToJson(iter: Iterator[Object]) : Iterator[String] = {
    val mapper = StreamsJacksonMapper.getInstance()
    iter.flatMap(item => toJson(item, mapper))
  }

  def mapApplyProcessor(iter: Iterator[StreamsDatum], processor: StreamsProcessor) : Iterator[StreamsDatum] = {
    val clone = deepCopy(processor)
    clone.prepare(null)
    val listIterator : Iterator[Iterator[StreamsDatum]] = iter.flatMap(item => applyProcessor(item, clone))
    val out = listIterator.flatten
    clone.cleanUp()
    return out
  }

  def mapApplyConfiguredProcessor(iter: Iterator[StreamsDatum], processor: StreamsProcessor, configuration: Object) : Iterator[StreamsDatum] = {
    val clone = deepCopy(processor)
    clone.prepare(configuration)
    val listIterator : Iterator[Iterator[StreamsDatum]] = iter.flatMap(item => applyProcessor(item, clone))
    val out = listIterator.flatten
    clone.cleanUp()
    return out
  }

  def writeDocumentAsString(in: StreamsDatum, mapper: ObjectMapper) : Option[String] = {
    val out = Try(mapper.writeValueAsString(in).trim)
    out match {
      case Success(v : String) =>
        if( out != null ) return Some(v) else return None
      case Failure(e : Throwable) =>
        LOGGER.warn(in.getId, e)
        return None
      case _ =>
        return None
    }
  }

  def writeDocumentAsString(iter: Iterator[StreamsDatum]) : Iterator[String] = {
    val mapper = StreamsJacksonMapper.getInstance()
    val out = iter.flatMap(item => writeDocumentAsString(item, mapper))
    return out
  }


  def applyPersistWriter(in: StreamsDatum, writer: StreamsPersistWriter): Unit = {
    writer.write(in)
  }

  def mapApplyProcessor(iter: Iterator[StreamsDatum], writer: StreamsPersistWriter) = {
    writer.prepare(null)
    iter.map(item => applyPersistWriter(item, writer))
    writer.cleanUp()
  }

  def deepCopy[A](a: A)(implicit m: reflect.Manifest[A]): A =
    SerializationUtil.cloneBySerialization(a)
}

