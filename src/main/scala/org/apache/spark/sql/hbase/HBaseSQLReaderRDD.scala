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

import org.apache.hadoop.hbase.client.{Get, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hbase.catalyst.expressions.HBaseMutableRows
import org.apache.spark.sql.hbase.execution.HBaseSQLTableScan
import org.apache.spark.sql.hbase.util.{BinaryBytesUtils, DataTypeUtils, HBaseKVHelper}
import org.apache.spark.sql.types.{AtomicType, DataType}

import scala.collection.mutable.ArrayBuffer

object CoprocessorConstants {
  final val COKEY: String = "coproc"
  final val COINDEX: String = "parIdx"
  final val COTYPE: String = "dtType"
  final val COTASK: String = "tskCtx"
}

/**
 *
 * @param relation the HBase relation
 * @param codegenEnabled whether codegen is in effect
 * @param useCustomFilter whether custom filter is in effect
 * @param output projection. For post coprocessor processing,
 *               is the projection of the original scan
 * @param subplan coprocessor subplan to be sent to coprocessor
 * @param dummyRDD in-memory scan RDD, might be used to reconstruct the original subplan.
 *                 This is possible when decision to use coprocessor has to be made
 *                 by the slaves when its partition-specific predicate is
 *                 determined, for efficiency reason by individual slaves and
 *                 not the driver. It can't be constructed and has to be sent over
 *                 by the driver for a RDD restriction
 * @param deploySuccessfully whether this jar is usable by HBase region servers
 * @param filterPred predicate pushed down in the physical plan
 * @param sqlContext SQL context
 */
class HBaseSQLReaderRDD(val relation: HBaseRelation,
                        val codegenEnabled: Boolean,
                        val useCustomFilter: Boolean,
                        val output: Seq[Attribute],
                        subplan: Option[SparkPlan],
                        dummyRDD: DummyRDD,
                        val deploySuccessfully: Option[Boolean],
                        @transient val filterPred: Option[Expression],
                        @transient sqlContext: SQLContext)
  extends RDD[InternalRow](sqlContext.sparkContext, Nil) with Logging {
  val hasSubPlan = subplan.isDefined
  val rowBuilder: (Seq[(Attribute, Int)], Result, MutableRow) => InternalRow = if (hasSubPlan) {
    relation.buildRowAfterCoprocessor
  } else {
    relation.buildRow
  }
  val newSubplanRDD: RDD[InternalRow] = if (hasSubPlan) {
    // Since HBase doesn't hold all information,
    // we need to execute the subplan in SparkSql first
    // and then send the executed subplanRDD to HBase
    val sp: SparkPlan = subplan.get
    val result = sp.execute()
    initDependencies(result)
    result
  } else {
    null
  }

  private val restoredSubplanRDD = if (hasSubPlan) {
    val rdd = subplan.get.transformUp {
      case HBaseSQLTableScan(relation: HBaseRelation, output: Seq[Attribute], _) =>
        HBaseSQLTableScan(relation, output, dummyRDD)
    }.execute()
    initDependencies(rdd)
    rdd
  } else {
    null
  }

  // Since the dependencies of RDD is a lazy val,
  // we need to initialize all its dependencies before sending it to HBase coprocessor
  def initDependencies(rdd: RDD[InternalRow]): Unit = {
    if (rdd.dependencies.nonEmpty) initDependencies(rdd.firstParent[InternalRow])
  }

  override def getPartitions: Array[Partition] = {
    RangeCriticalPoint.generatePrunedPartitions(relation, filterPred).toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map {
      identity
    }.toSeq
  }

  /**
   * construct row key based on the critical point range information
   * @param cpr the critical point range
   * @param isStart the switch between start and end value
   * @return the encoded row key, or null if the value is None
   */
  private def constructRowKey(cpr: MDCriticalPointRange[_], isStart: Boolean): HBaseRawType = {
    val prefix = cpr.prefix
    val head: Seq[(HBaseRawType, AtomicType)] = prefix.map {
      case (itemValue, itemType) =>
        (DataTypeUtils.dataToBytes(itemValue, itemType), itemType)
    }

    val key = if (isStart) cpr.lastRange.start else cpr.lastRange.end
    val keyType = cpr.lastRange.dt
    val list = if (key.isDefined) {
      val tail: (HBaseRawType, AtomicType) = {
        (DataTypeUtils.dataToBytes(key.get, keyType), keyType)
      }
      head :+ tail
    } else {
      head
    }
    if (list.isEmpty) {
      null
    } else {
      HBaseKVHelper.encodingRawKeyColumns(list)
    }
  }

  private def createIterator(context: TaskContext,
                             scanner: ResultScanner,
                             otherFilters: Option[Expression]): Iterator[InternalRow] = {
    val finalOutput = if (hasSubPlan) {
      subplan.get.output
    } else if (otherFilters.isDefined) {
      output.union(otherFilters.get.references.toSeq).distinct
    } else {
      output.distinct
    }

    val row = new HBaseMutableRows(finalOutput.size)
    val projections = finalOutput.zipWithIndex

    var finished: Boolean = false
    var gotNext: Boolean = false
    var result: Result = null

    val otherFilter: (InternalRow) => Boolean =
      if (!hasSubPlan && otherFilters.isDefined) {
        if (codegenEnabled) {
          GeneratePredicate.generate(otherFilters.get, finalOutput)
        } else {
          InterpretedPredicate.create(otherFilters.get, finalOutput)
        }
      } else null

    val iterator = new Iterator[InternalRow] {
      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            result = scanner.next
            finished = result == null
            gotNext = true
          }
        }
        if (finished) {
          close()
        }
        !finished
      }

      override def next(): InternalRow = {
        if (hasNext) {
          gotNext = false
          rowBuilder(projections, result, row)
        } else {
          null
        }
      }

      def close() = {
        try {
          scanner.close()
          relation.closeHTable()
        } catch {
          case e: Exception => logWarning("Exception in scanner.close", e)
        }
      }
    }

    if (otherFilter == null) {
      new InterruptibleIterator(context, iterator)
    } else {
      new InterruptibleIterator(context, iterator.filter(otherFilter))
    }
  }

  private def setCoprocessor(scan: Scan, otherFilters: Option[Expression], partitionIndex: Int) = {
    subplan.get.transformUp {
      case s: HBaseSQLTableScan =>
        val rdd = s.result.asInstanceOf[HBaseCoprocessorSQLReaderRDD]
        rdd.relation = relation
        rdd.otherFilters = None
        rdd.finalOutput = rdd.finalOutput.distinct
        if (otherFilters.isDefined) {
          rdd.finalOutput = rdd.finalOutput.union(otherFilters.get.references.toSeq)
        }
        s
    }

    if (!useCustomFilter) {
      def addOtherFilter(rdd: RDD[InternalRow]): Unit = rdd match {
        case hcsRDD: HBaseCoprocessorSQLReaderRDD => hcsRDD.otherFilters = otherFilters
        case _ => if (rdd.dependencies.nonEmpty) addOtherFilter(rdd.firstParent[InternalRow])
      }
      addOtherFilter(newSubplanRDD)
    }

    val outputDataType: Seq[DataType] = subplan.get.output.map(attr => attr.dataType)
    val taskContextPara: (Int, Int, Long, Int) = TaskContext.get() match {
      case t: TaskContextImpl => (t.stageId, t.partitionId, t.taskAttemptId, t.attemptNumber)
      case _ => (0, 0, 0L, 0)
    }

    scan.setAttribute(CoprocessorConstants.COINDEX, Bytes.toBytes(partitionIndex))
    scan.setAttribute(CoprocessorConstants.COTYPE, HBaseSerializer.serialize(outputDataType))
    scan.setAttribute(CoprocessorConstants.COKEY, HBaseSerializer.serialize(newSubplanRDD))
    scan.setAttribute(CoprocessorConstants.COTASK, HBaseSerializer.serialize(taskContextPara))
  }

  // For critical-point-based predicate pushdown
  // partial reduction for those partitions mapped to multiple critical point ranges,
  // as indicated by the keyPartialEvalIndex in the partition, where the original
  // filter predicate will be used
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partition = split.asInstanceOf[HBasePartition]
    val predicate = partition.computePredicate(relation)
    val expandedCPRs: Seq[MDCriticalPointRange[_]] =
      RangeCriticalPoint.generateCriticalPointRanges(relation, predicate)._2.
        flatMap(_.flatten(new ArrayBuffer[(Any, AtomicType)](relation.dimSize)))

    if (expandedCPRs.isEmpty) {
      val (filters, otherFilters) = relation.buildPushdownFilterList(predicate)
      val scan = relation.buildScan(partition.start, partition.end, predicate, filters,
        otherFilters, useCustomFilter, output)
      if (hasSubPlan) setCoprocessor(scan, otherFilters, split.index)
      val scanner = relation.htable.getScanner(scan)
      if ((useCustomFilter || hasSubPlan) && deploySuccessfully.getOrElse(false)) {
        // other filters will be evaluated as part of a custom filter
        // or nonexistent in post coprocessor execution
        createIterator(context, scanner, None)
      } else {
        createIterator(context, scanner, otherFilters)
      }
    } else {
      // expandedCPRs is not empty
      val isPointRanges = expandedCPRs.forall(
        p => p.lastRange.isPoint && p.prefix.size == relation.keyColumns.size - 1)
      if (isPointRanges) {
        // all of the last ranges are point range, build a list of get
        val gets: java.util.List[Get] = new java.util.ArrayList[Get]()

        val distinctProjectionList = output.distinct
        val nonKeyColumns = relation.nonKeyColumns.filter {
          case nkc => distinctProjectionList.exists(nkc.sqlName == _.name)
        }

        def generateGet(range: MDCriticalPointRange[_]): Get = {
          val rowKey = constructRowKey(range, isStart = true)
          val get = new Get(rowKey)
          for (nonKeyColumn <- nonKeyColumns) {
            get.addColumn(Bytes.toBytes(nonKeyColumn.family), Bytes.toBytes(nonKeyColumn.qualifier))
          }
          get
        }
        val predForEachRange: Seq[Expression] = expandedCPRs.map(range => {
          gets.add(generateGet(range))
          range.lastRange.pred
        })
        val resultsWithPred = relation.htable.get(gets).zip(predForEachRange).filter(!_._1.isEmpty)

        def evalResultForBoundPredicate(input: InternalRow, predicate: Expression): Boolean = {
          val boundPredicate = BindReferences.bindReference(predicate, output)
          boundPredicate.eval(input).asInstanceOf[Boolean]
        }
        val projections = output.zipWithIndex
        val resultRows: Seq[InternalRow] = for {
          (result, predicate) <- resultsWithPred
          row = new HBaseMutableRows(output.size)
          resultRow = relation.buildRow(projections, result, row)
          if predicate == null || evalResultForBoundPredicate(resultRow, predicate)
        } yield resultRow

        val result = resultRows.toIterator
        if (hasSubPlan) {
          // restore the original subplan using the dummyRDD
          dummyRDD.result = result
          restoredSubplanRDD.compute(split, context)
        } else {
          result
        }
      }
      else {
        // isPointRanges is false
        // calculate the range start
        val startRowKey = constructRowKey(expandedCPRs.head, isStart = true)
        val start = if (startRowKey != null) {
          if (partition.start.isDefined && Bytes.compareTo(partition.start.get, startRowKey) > 0) {
            Some(partition.start.get)
          } else {
            Some(startRowKey)
          }
        } else {
          partition.start
        }

        // calculate the range end
        val size = expandedCPRs.size - 1
        val endKey: Option[Any] = expandedCPRs(size).lastRange.end
        val endInclusive: Boolean = expandedCPRs(size).lastRange.endInclusive
        val endRowKey = constructRowKey(expandedCPRs(size), isStart = false)
        val end = if (endRowKey != null) {
          val finalKey: HBaseRawType = {
            if (endInclusive || endKey.isEmpty) {
              BinaryBytesUtils.addOne(endRowKey)
            } else {
              endRowKey
            }
          }

          if (finalKey != null) {
            if (partition.end.isDefined && Bytes.compareTo(finalKey, partition.end.get) > 0) {
              Some(partition.end.get)
            } else {
              Some(finalKey)
            }
          } else {
            partition.end
          }
        } else {
          partition.end
        }


        val (filters, otherFilters) =
          relation.buildCPRFilterList(output, predicate, expandedCPRs)
        val scan = relation.buildScan(start, end, predicate, filters,
          otherFilters, useCustomFilter, output)
        if (hasSubPlan) setCoprocessor(scan, otherFilters, split.index)
        val scanner = relation.htable.getScanner(scan)
        if ((useCustomFilter || hasSubPlan) && deploySuccessfully.getOrElse(false)) {
          // other filters will be evaluated as part of a custom filter
          // or nonexistent in post coprocessor execution
          createIterator(context, scanner, None)
        } else {
          createIterator(context, scanner, otherFilters)
        }
      }
    }
  }
}

private[hbase] class DummyRDD(@transient sqlContext: SQLContext)
  extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  @transient var result: Iterator[InternalRow] = _

  override def getPartitions = ???

  override def getPreferredLocations(split: Partition) = ???

  override def compute(split: Partition, taskContext: TaskContext): Iterator[InternalRow] = {
    result
  }
}
