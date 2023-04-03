/*
 * Copyright 2023 mixayloff-dimaaylov at github dot com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.infocom.examples.spark.serialization

import java.util

import org.apache.avro.io.DecoderFactory
import org.apache.avro.reflect.{ ReflectData, ReflectDatumReader }
import org.apache.kafka.common.serialization.Deserializer

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
class AvroDataPointDeserializer[T] extends Deserializer[T]() {
  @transient private var reflectDatumReader: ReflectDatumReader[T] = _

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val t = configs.get(s"${if (isKey) "key" else "value"}.deserializer.type").asInstanceOf[Class[T]]
    val schema = ReflectData.get().getSchema(t)
    reflectDatumReader = new ReflectDatumReader[T](schema)
  }

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): T = {
    val decoder = DecoderFactory.get().binaryDecoder(data, null)
    reflectDatumReader.read(null.asInstanceOf[T], decoder)
  }
}
