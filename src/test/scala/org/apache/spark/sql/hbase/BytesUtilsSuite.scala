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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.apache.spark.sql.hbase.types.HBaseBytesType
import org.apache.spark.sql.hbase.util.BinaryBytesUtils
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class BytesUtilsSuite extends FunSuite with BeforeAndAfterAll with Logging {
  test("Bytes Ordering Test") {
    val s = Seq(-257, -256, -255, -129, -128, -127, -64, -16, -4, -1,
      0, 1, 4, 16, 64, 127, 128, 129, 255, 256, 257)
    val result = s.map(i => (i, BinaryBytesUtils.create(IntegerType).toBytes(i)))
      .sortWith((f, s) =>
      HBaseBytesType.ordering.gt(
        f._2.asInstanceOf[HBaseBytesType.InternalType],
        s._2.asInstanceOf[HBaseBytesType.InternalType]))
    assert(result.map(a => a._1) == s.sorted.reverse)
  }

  def compare(a: Array[Byte], b: Array[Byte]): Int = {
    val length = Math.min(a.length, b.length)
    var result: Int = 0
    for (i <- 0 to length - 1) {
      val diff: Int = (a(i) & 0xff).asInstanceOf[Byte] - (b(i) & 0xff).asInstanceOf[Byte]
      if (diff != 0) {
        result = diff
      }
    }
    result
  }

  test("Bytes Utility Test") {
    assert(BinaryBytesUtils.toBoolean(BinaryBytesUtils.create(BooleanType)
      .toBytes(input = true), 0) === true)
    assert(BinaryBytesUtils.toBoolean(BinaryBytesUtils.create(BooleanType)
      .toBytes(input = false), 0) === false)

    assert(BinaryBytesUtils.toDouble(BinaryBytesUtils.create(DoubleType).toBytes(12.34d), 0)
      === 12.34d)
    assert(BinaryBytesUtils.toDouble(BinaryBytesUtils.create(DoubleType).toBytes(-12.34d), 0)
      === -12.34d)

    assert(BinaryBytesUtils.toFloat(BinaryBytesUtils.create(FloatType).toBytes(12.34f), 0)
      === 12.34f)
    assert(BinaryBytesUtils.toFloat(BinaryBytesUtils.create(FloatType).toBytes(-12.34f), 0)
      === -12.34f)

    assert(BinaryBytesUtils.toInt(BinaryBytesUtils.create(IntegerType).toBytes(12), 0)
      === 12)
    assert(BinaryBytesUtils.toInt(BinaryBytesUtils.create(IntegerType).toBytes(-12), 0)
      === -12)

    assert(BinaryBytesUtils.toLong(BinaryBytesUtils.create(LongType).toBytes(1234l), 0)
      === 1234l)
    assert(BinaryBytesUtils.toLong(BinaryBytesUtils.create(LongType).toBytes(-1234l), 0)
      === -1234l)

    assert(BinaryBytesUtils.toShort(BinaryBytesUtils.create(ShortType)
      .toBytes(12.asInstanceOf[Short]), 0) === 12)
    assert(BinaryBytesUtils.toShort(BinaryBytesUtils.create(ShortType)
      .toBytes(-12.asInstanceOf[Short]), 0) === -12)

    assert(BinaryBytesUtils.toUTF8String(BinaryBytesUtils.create(StringType).toBytes("abc"), 0, 3).toString
      == "abc")
    assert(BinaryBytesUtils.toUTF8String(BinaryBytesUtils.create(StringType).toBytes(""), 0, 0).toString == "")

    assert(BinaryBytesUtils.toByte(BinaryBytesUtils.create(ByteType)
      .toBytes(5.asInstanceOf[Byte]), 0) === 5)
    assert(BinaryBytesUtils.toByte(BinaryBytesUtils.create(ByteType)
      .toBytes(-5.asInstanceOf[Byte]), 0) === -5)

    assert(compare(BinaryBytesUtils.create(IntegerType).toBytes(128),
      BinaryBytesUtils.create(IntegerType).toBytes(-128)) > 0)
  }

  test("byte array plus one") {
    var byteArray = Array[Byte](0x01.toByte, 127.toByte)
    assert(Bytes.compareTo(BinaryBytesUtils.addOne(byteArray), Array[Byte](0x01.toByte, 0x80.toByte)) == 0)

    byteArray = Array[Byte](0xff.toByte, 0xff.toByte)
    assert(BinaryBytesUtils.addOne(byteArray) == null)

    byteArray = Array[Byte](0x02.toByte, 0xff.toByte)
    assert(Bytes.compareTo(BinaryBytesUtils.addOne(byteArray), Array[Byte](0x03.toByte, 0x00.toByte)) == 0)
  }

  test("float comparison") {
    val f1 = BinaryBytesUtils.create(FloatType).toBytes(-1.23f)
    val f2 = BinaryBytesUtils.create(FloatType).toBytes(100f)
    assert(Bytes.compareTo(f1, f2) < 0)
  }
}
