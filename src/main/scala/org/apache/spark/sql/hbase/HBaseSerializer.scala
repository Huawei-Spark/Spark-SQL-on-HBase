/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hbase

import java.io._

import org.apache.hadoop.hbase.{KeyValue, CellUtil, Cell}
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter.FilterBase
import org.apache.hadoop.hbase.util.{Bytes, Writables}
import org.apache.hadoop.io.Writable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hbase.util.{HBaseKVHelper, DataTypeUtils, BinaryBytesUtils}
import org.apache.spark.sql.types.{DataType, AtomicType, StringType}
import org.apache.spark.sql.hbase.catalyst.expressions.PartialPredicateOperations._

/**
 * the serializer to serialize / de-serialize the objects for HBase embedded execution,
 * may be made configurable and use the ones provided by Spark in the future.
 */
private[hbase] object HBaseSerializer {
  /**
   * serialize the input object to byte array
   * @param obj the input object
   * @return the serialized byte array
   */
  def serialize(obj: Any): Array[Byte] = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(obj)
    val res = b.toByteArray
    o.close()
    b.close()
    res
  }

  /**
   * de-serialize the byte array to the original object
   * @param bytes the input byte array
   * @return the de-serialized object
   */
  def deserialize(bytes: Array[Byte]): Any = {
    val b = new ByteArrayInputStream(bytes)
    val o = new ObjectInputStream(b)
    val res = o.readObject()
    o.close()
    b.close()
    res
  }
}
