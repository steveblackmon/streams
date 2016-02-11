/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.streams.spark

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.Config
import org.apache.streams.converter.{LineReadWriteConfiguration, LineReadWriteUtil}
import org.apache.streams.core.{StreamsPersistWriter, StreamsDatum, StreamsProcessor}
import org.apache.streams.jackson.StreamsJacksonMapper
import org.apache.streams.local.builders.LocalStreamBuilder
import org.apache.streams.pojo.json.{Actor, ActivityObject, Activity}
import org.apache.streams.util.SerializationUtil
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object StreamsSparkBuilder {

  private val LOGGER: Logger = LoggerFactory.getLogger("StreamsSparkHelper")

  def isValidJson(in: String) : Boolean = {
    val mapper = StreamsJacksonMapper.getInstance()
    val obj = Try(mapper.readValue(in.asInstanceOf[String], classOf[ObjectNode]))
    obj match {
      case Success(v : ObjectNode) =>
        return true
      case Failure(e : Throwable) =>
        return false
      case _ =>
        return false
    }
  }

  def validateJson(in: String) : Option[String] = {
    if( isValidJson(in) )
      return Some(in)
    else
      return None
  }

  def mapValidateJson(iter: Iterator[String]) : Iterator[String] = {
    iter.flatMap(item => validateJson(item))
  }

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

  def mapAsDatum(iter: Iterator[Object]) : Iterator[StreamsDatum] = {
    iter.flatMap(item => asDatum(item))
  }

  def setId(datum: StreamsDatum, in: Actor) : StreamsDatum = {
    return datum.setId(in.getId).asInstanceOf[StreamsDatum]
  }

  def setTimestamp(datum: StreamsDatum, in: Actor) : StreamsDatum = {
    val ts : DateTime  = in.getPublished
    return datum.setTimestamp(ts).asInstanceOf[StreamsDatum]
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

  def mapDatumDocAsString(iter: Iterator[StreamsDatum]) : Iterator[String] = {
    val mapper = StreamsJacksonMapper.getInstance()
    iter.flatMap(item => datumDocAsString(item, mapper))
  }

  def activityObjectAsDatum(in: ActivityObject) : Option[StreamsDatum] = {
    val out = Try(new StreamsDatum(in, in.getId, in.getPublished))
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

  def mapActivityObjectAsDatum(iter: Iterator[ActivityObject]) : Iterator[StreamsDatum] = {
    iter.flatMap(item => activityObjectAsDatum(item))
  }

  def activityAsDatum(in: Activity) : Option[StreamsDatum] = {
    val out = Try(new StreamsDatum(in, in.getId, in.getPublished))
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

  def mapActivityAsDatum(iter: Iterator[Activity]) : Iterator[StreamsDatum] = {
    iter.flatMap(item => activityAsDatum(item))
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

  def mapWriteDocumentAsString(iter: Iterator[StreamsDatum]) : Iterator[String] = {
    val mapper = StreamsJacksonMapper.getInstance()
    val out = iter.flatMap(item => writeDocumentAsString(item, mapper))
    return out
  }

  def writeDocumentAsMap(in: StreamsDatum) : Option[Map[String,AnyRef]] = {
    val mapper = StreamsJacksonMapper.getInstance()
    mapper.registerModule(DefaultScalaModule)
    val doc = in.getDocument
    var out : Try[Map[String,AnyRef]] = null
    if( doc.isInstanceOf[String])
      out = Try(mapper.readValue(doc.asInstanceOf[String], classOf[Map[String,AnyRef]]))
    else
      out = Try(mapper.convertValue(doc, classOf[Map[String,AnyRef]]))
    out match {
      case Success(v : Map[String,AnyRef]) =>
        return Some(out.get)
      case Failure(e : Throwable) =>
        LOGGER.warn(in.getId, e)
        return None
      case _ =>
        return None
    }
  }

  def asLine(in: StreamsDatum, lineReadWriteConfiguration: LineReadWriteConfiguration = new LineReadWriteConfiguration()) : Option[String] = {
    val lineReadWriteUtil = LineReadWriteUtil.getInstance(lineReadWriteConfiguration)
    val out = Try(lineReadWriteUtil.convertResultToString(in))
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

  def mapAsLine(iter: Iterator[StreamsDatum], lineReadWriteConfiguration: LineReadWriteConfiguration = new LineReadWriteConfiguration()) : Iterator[String] = {
    val out = iter.flatMap(item => asLine(item, lineReadWriteConfiguration))
    return out
  }

  def fromLine(in: String, lineReadWriteConfiguration: LineReadWriteConfiguration = new LineReadWriteConfiguration()) : Option[StreamsDatum] = {
    val lineReadWriteUtil = LineReadWriteUtil.getInstance(lineReadWriteConfiguration)
    val out = Try(lineReadWriteUtil.processLine(in))
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

  def mapFromLine(iter: Iterator[String], lineReadWriteConfiguration: LineReadWriteConfiguration = new LineReadWriteConfiguration()) : Iterator[StreamsDatum] = {
    val out = iter.flatMap(item => fromLine(item, lineReadWriteConfiguration))
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
