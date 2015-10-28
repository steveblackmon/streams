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

import java.util

import com.esotericsoftware.kryo.Kryo
import com.google.common.collect.Lists
import org.apache.spark.serializer.KryoRegistrator
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.util.{ClasspathHelper, ConfigurationBuilder}

import scala.collection.JavaConverters._

/**
 * Created by steve on 9/16/15.
 */
class StreamsKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    val reflections: Reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("org.apache.streams")).setScanners(new SubTypesScanner))
    val serializableClasses: util.Set[Class[_ <: Serializable]] = reflections.getSubTypesOf(classOf[Serializable])
    Lists.newArrayList(serializableClasses).asScala.foreach(
      klass => kryo.register(klass)
    )
  }
}
