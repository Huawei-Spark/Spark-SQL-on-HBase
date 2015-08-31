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

package org.apache.spark.sql.hbase.util

import com.google.protobuf.InvalidProtocolBufferException
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos
import org.apache.hadoop.hbase.util.{ByteStringer, Bytes}
import org.apache.spark.sql.hbase._

class CustomComparator(value: Array[Byte]) extends ByteArrayComparable(value) {
  def compareTo(value: HBaseRawType, offset: Int, length: Int): Int = {
    Bytes.compareTo(this.value, 0, this.value.length, value, offset, length)
  }

  def origConvert: ComparatorProtos.ByteArrayComparable = {
    val builder = ComparatorProtos.ByteArrayComparable.newBuilder
    if (value != null) builder.setValue(ByteStringer.wrap(value))
    builder.build
  }

  /**
   * @return The comparator serialized using pb
   */
  def toByteArray: Array[Byte] = {
    val builder = AdditionalComparators.CustomComparator.newBuilder()
    builder.setComparable(origConvert)
    builder.build.toByteArray
  }
}

object IntComparator {
  /**
   * @param pbBytes A pb serialized { @link IntComparator} instance
   * @return An instance of { @link IntComparator} made from <code>bytes</code>
   * @throws DeserializationException exception
   * @see #toByteArray
   */
  def parseFrom(pbBytes: Array[Byte]): IntComparator = {
    var proto: AdditionalComparators.CustomComparator = null
    try {
      proto = AdditionalComparators.CustomComparator.parseFrom(pbBytes)
    }
    catch {
      case e: InvalidProtocolBufferException =>
        throw new DeserializationException(e)
    }
    new IntComparator(proto.getComparable.getValue.toByteArray)
  }
}

class IntComparator(value: Array[Byte]) extends CustomComparator(value) {
  override def compareTo(value: HBaseRawType, offset: Int, length: Int): Int = {
    StringBytesUtils.toInt(this.value, 0, this.value.length) -
      StringBytesUtils.toInt(value, offset, length)
  }
}

object ByteComparator {
  /**
   * @param pbBytes A pb serialized { @link IntComparator} instance
   * @return An instance of { @link IntComparator} made from <code>bytes</code>
   * @throws DeserializationException exception
   * @see #toByteArray
   */
  def parseFrom(pbBytes: Array[Byte]): ByteComparator = {
    var proto: AdditionalComparators.CustomComparator = null
    try {
      proto = AdditionalComparators.CustomComparator.parseFrom(pbBytes)
    }
    catch {
      case e: InvalidProtocolBufferException =>
        throw new DeserializationException(e)
    }
    new ByteComparator(proto.getComparable.getValue.toByteArray)
  }
}

class ByteComparator(value: Array[Byte]) extends CustomComparator(value) {
  override def compareTo(value: HBaseRawType, offset: Int, length: Int): Int = {
    StringBytesUtils.toByte(this.value, 0, this.value.length) -
      StringBytesUtils.toByte(value, offset, length)
  }
}

object ShortComparator {
  /**
   * @param pbBytes A pb serialized { @link IntComparator} instance
   * @return An instance of { @link IntComparator} made from <code>bytes</code>
   * @throws DeserializationException exception
   * @see #toByteArray
   */
  def parseFrom(pbBytes: Array[Byte]): ShortComparator = {
    var proto: AdditionalComparators.CustomComparator = null
    try {
      proto = AdditionalComparators.CustomComparator.parseFrom(pbBytes)
    }
    catch {
      case e: InvalidProtocolBufferException =>
        throw new DeserializationException(e)
    }
    new ShortComparator(proto.getComparable.getValue.toByteArray)
  }
}

class ShortComparator(value: Array[Byte]) extends CustomComparator(value) {
  override def compareTo(value: HBaseRawType, offset: Int, length: Int): Int = {
    StringBytesUtils.toShort(this.value, 0, this.value.length) -
      StringBytesUtils.toShort(value, offset, length)
  }
}

object LongComparator {
  /**
   * @param pbBytes A pb serialized { @link IntComparator} instance
   * @return An instance of { @link IntComparator} made from <code>bytes</code>
   * @throws DeserializationException exception
   * @see #toByteArray
   */
  def parseFrom(pbBytes: Array[Byte]): LongComparator = {
    var proto: AdditionalComparators.CustomComparator = null
    try {
      proto = AdditionalComparators.CustomComparator.parseFrom(pbBytes)
    }
    catch {
      case e: InvalidProtocolBufferException =>
        throw new DeserializationException(e)
    }
    new LongComparator(proto.getComparable.getValue.toByteArray)
  }
}

class LongComparator(value: Array[Byte]) extends CustomComparator(value) {
  override def compareTo(value: HBaseRawType, offset: Int, length: Int): Int = {
    val r = StringBytesUtils.toLong(this.value, 0, this.value.length) -
      StringBytesUtils.toLong(value, offset, length)
    if (r > 0) 1
    else if (r == 0) 0
    else -1
  }
}

object DoubleComparator {
  /**
   * @param pbBytes A pb serialized { @link IntComparator} instance
   * @return An instance of { @link IntComparator} made from <code>bytes</code>
   * @throws DeserializationException exception
   * @see #toByteArray
   */
  def parseFrom(pbBytes: Array[Byte]): DoubleComparator = {
    var proto: AdditionalComparators.CustomComparator = null
    try {
      proto = AdditionalComparators.CustomComparator.parseFrom(pbBytes)
    }
    catch {
      case e: InvalidProtocolBufferException =>
        throw new DeserializationException(e)
    }
    new DoubleComparator(proto.getComparable.getValue.toByteArray)
  }
}

class DoubleComparator(value: Array[Byte]) extends CustomComparator(value) {
  override def compareTo(value: HBaseRawType, offset: Int, length: Int): Int = {
    val r = StringBytesUtils.toDouble(this.value, 0, this.value.length) -
      StringBytesUtils.toDouble(value, offset, length)
    if (r > 0) 1
    else if (r == 0) 0
    else -1
  }
}

object FloatComparator {
  /**
   * @param pbBytes A pb serialized { @link IntComparator} instance
   * @return An instance of { @link IntComparator} made from <code>bytes</code>
   * @throws DeserializationException exception
   * @see #toByteArray
   */
  def parseFrom(pbBytes: Array[Byte]): FloatComparator = {
    var proto: AdditionalComparators.CustomComparator = null
    try {
      proto = AdditionalComparators.CustomComparator.parseFrom(pbBytes)
    }
    catch {
      case e: InvalidProtocolBufferException =>
        throw new DeserializationException(e)
    }
    new FloatComparator(proto.getComparable.getValue.toByteArray)
  }
}

class FloatComparator(value: Array[Byte]) extends CustomComparator(value) {
  override def compareTo(value: HBaseRawType, offset: Int, length: Int): Int = {
    val r = StringBytesUtils.toFloat(this.value, 0, this.value.length) -
      StringBytesUtils.toFloat(value, offset, length)
    if (r > 0) 1
    else if (r == 0) 0
    else -1
  }
}

object BoolComparator {
  /**
   * @param pbBytes A pb serialized { @link IntComparator} instance
   * @return An instance of { @link IntComparator} made from <code>bytes</code>
   * @throws DeserializationException exception
   * @see #toByteArray
   */
  def parseFrom(pbBytes: Array[Byte]): BoolComparator = {
    var proto: AdditionalComparators.CustomComparator = null
    try {
      proto = AdditionalComparators.CustomComparator.parseFrom(pbBytes)
    }
    catch {
      case e: InvalidProtocolBufferException =>
        throw new DeserializationException(e)
    }
    new BoolComparator(proto.getComparable.getValue.toByteArray)
  }
}

class BoolComparator(value: Array[Byte]) extends CustomComparator(value) {
  override def compareTo(value: HBaseRawType, offset: Int, length: Int): Int = {
    if (StringBytesUtils.toBoolean(this.value, 0, this.value.length)) 1
    else -1
  }
}
