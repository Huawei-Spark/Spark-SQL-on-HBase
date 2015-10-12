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

import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hbase.catalyst.expressions.HBaseMutableRows
import org.apache.spark.sql.hbase.catalyst.expressions.PartialPredicateOperations._
import org.apache.spark.sql.hbase.types.{HBaseBytesType, Range}
import org.apache.spark.{Logging, Partition}


private[hbase] class HBasePartition(
                                     val idx: Int, val mappedIndex: Int,
                                     start: Option[HBaseRawType] = None,
                                     end: Option[HBaseRawType] = None,
                                     val server: Option[String] = None,
                                     val filterPredicates: Option[Expression] = None,
                                     @transient relation: HBaseRelation = null,
                                     @transient val newScanner: RegionScanner = null)
  extends Range[HBaseRawType](start, true, end, false, HBaseBytesType)
  with Partition with IndexMappable with Logging {

  override def index: Int = idx

  override def hashCode(): Int = idx

  @transient lazy val startNative: Seq[Any] = relation.nativeKeyConvert(start)

  @transient lazy val endNative: Seq[Any] = relation.nativeKeyConvert(end)

  /** Compute predicate specific for this partition: performed by the Spark slaves
    *
    * @param relation The HBase relation
    * @return the partition-specific predicate
    */
  def computePredicate(relation: HBaseRelation): Option[Expression] = {
    val predicate = if (filterPredicates.isDefined &&
      filterPredicates.get.references.exists(_.exprId == relation.partitionKeys.head.exprId)) {
      val oriPredicate = filterPredicates.get
      val predicateReferences = oriPredicate.references.toSeq
      val boundReference = BindReferences.bindReference(oriPredicate, predicateReferences)
      val row = new HBaseMutableRows(predicateReferences.size)
      var rowIndex = 0
      var i = 0
      var range: Range[_] = null
      while (i < relation.keyColumns.size) {
        range = relation.generateRange(this, oriPredicate, i)
        if (range != null) {
          rowIndex = relation.rowIndex(predicateReferences, i)
          if (rowIndex >= 0) row.update(rowIndex, range)
          // if the non-last dimension range is not point, do not proceed to the next dims
          if (i < relation.keyColumns.size - 1 && !range.isPoint) i = relation.keyColumns.size
          else i = i + 1
        } else i = relation.keyColumns.size
      }
      val pr = boundReference.partialReduce(row, predicateReferences)
      pr match {
        case (null, e: Expression) => Some(e)
        case (true, _) => None
        case (false, _) => Some(Literal(false))
      }
    } else filterPredicates
    logInfo(predicate.toString)
    predicate
  }

  override def toString = {
    s"HBasePartition: $idx, $mappedIndex, [$start, $end), $filterPredicates"
  }
}
