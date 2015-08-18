package org.apache.streams.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.Config
import org.apache.streams.core.{StreamsPersistWriter, StreamsDatum, StreamsProcessor}
import org.apache.streams.hdfs.{WebHdfsPersistReader, WebHdfsPersistWriter}
import org.apache.streams.jackson.StreamsJacksonMapper
import org.apache.streams.local.builders.LocalStreamBuilder
import org.apache.streams.pojo.json.{ActivityObject, Activity}
import org.apache.streams.util.SerializationUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object StreamsSparkBuilder {

  private val LOGGER: Logger = LoggerFactory.getLogger("StreamsSparkHelper")

  def asActivity(in: Object) : Option[Activity] = {
    val mapper = StreamsJacksonMapper.getInstance()
    if( in.isInstanceOf[Activity] )
      return Some(in.asInstanceOf[Activity])
    else if( in.isInstanceOf[String] ) {
      val out = Try(mapper.readValue(in.asInstanceOf[String], classOf[Activity]))
      out match {
        case Success(v : Activity) =>
          return Some(v)
        case Failure(e : Throwable) =>
          LOGGER.warn(in.toString)
          return None
        case _ =>
          return None
      }
    }
    else {
      val out = Try(mapper.convertValue(in, classOf[Activity]))
      out match {
        case Success(v : Activity) =>
          return Some(v)
        case Failure(e : Throwable) =>
          LOGGER.warn(in.toString)
          return None
        case _ =>
          return None
      }
    }
  }

  def mapAsActivity(iter: Iterator[Object]) : Iterator[Activity] = {
    val mapper = StreamsJacksonMapper.getInstance()
    iter.flatMap(item => asActivity(item))
  }

  def asActivityObject(in: Object) : Option[ActivityObject] = {
    val mapper = StreamsJacksonMapper.getInstance()
    if( in.isInstanceOf[ActivityObject] )
      return Some(in.asInstanceOf[ActivityObject])
    else if( in.isInstanceOf[String] ) {
      val out = Try(mapper.readValue(in.asInstanceOf[String], classOf[ActivityObject]))
      out match {
        case Success(v : ActivityObject) =>
          return Some(v)
        case Failure(e : Throwable) =>
          LOGGER.warn(in.toString)
          return None
        case _ =>
          return None
      }
    }
    else {
      val out = Try(mapper.convertValue(in, classOf[ActivityObject]))
      out match {
        case Success(v : ActivityObject) =>
          return Some(v)
        case Failure(e : Throwable) =>
          LOGGER.warn(in.toString)
          return None
        case _ =>
          return None
      }
    }
  }

  def mapAsActivityObject(iter: Iterator[Object]) : Iterator[ActivityObject] = {
    val mapper = StreamsJacksonMapper.getInstance()
    iter.flatMap(item => asActivityObject(item))
  }

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

  def mapApplyProcessor(iter: Iterator[StreamsDatum], processor: StreamsProcessor) : Iterator[StreamsDatum] = {
    val clone = deepCopy(processor)
    clone.prepare(null)
    val listIterator : Iterator[Iterator[StreamsDatum]] = iter.flatMap(item => applyProcessor(item, clone))
    val out = listIterator.flatten
    clone.cleanUp()
    return out
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

  def asLine(in: StreamsDatum, webHdfsPersistWriter: WebHdfsPersistWriter) : Option[String] = {
    val out = Try(webHdfsPersistWriter.convertResultToString(in).trim)
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

  def asLine(iter: Iterator[StreamsDatum], webHdfsPersistWriter: WebHdfsPersistWriter) : Iterator[String] = {
    webHdfsPersistWriter.prepare(null)
    val out = iter.flatMap(item => asLine(item, webHdfsPersistWriter))
    webHdfsPersistWriter.cleanUp()
    return out
  }

  def fromLine(in: String, webHdfsPersistReader: WebHdfsPersistReader) : Option[StreamsDatum] = {
    val out = Try(webHdfsPersistReader.processLine(in))
    out match {
      case Success(v : StreamsDatum) =>
        if( out != null ) return Some(v) else return None
      case Failure(e : Throwable) =>
        LOGGER.warn(in, e)
        return None
      case _ =>
        return None
    }
  }

  def fromLine(iter: Iterator[String], webHdfsPersistReader: WebHdfsPersistReader) : Iterator[StreamsDatum] = {
    webHdfsPersistReader.prepare(null)
    val out = iter.flatMap(item => fromLine(item, webHdfsPersistReader))
    webHdfsPersistReader.cleanUp()
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

