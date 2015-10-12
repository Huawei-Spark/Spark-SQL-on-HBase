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

package org.apache.spark.sql.hbase.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BaseGenericInternalRow, MutableRow}
import org.apache.spark.sql.types.DataType

class HBaseMutableRows(values: Array[Any]) extends MutableRow with BaseGenericInternalRow {
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def genericGet(ordinal: Int) = values(ordinal)

  override def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = values

  override def numFields: Int = values.length

  override def setNullAt(i: Int): Unit = {
    values(i) = null
  }

  override def update(i: Int, value: Any): Unit = {
    values(i) = value
  }

  override def copy(): InternalRow = new HBaseMutableRows(values.clone())

}
