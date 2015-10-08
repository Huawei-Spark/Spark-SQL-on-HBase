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

package org.apache.spark.sql.hbase.execution

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{Project, SparkPlan}
import org.apache.spark.sql.hbase.{HBasePartition, HBaseRawType, HBaseRelation, KeyColumn}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Strategy, execution}

/**
 * Retrieves data using a HBaseTableScan.  Partition pruning predicates are also detected and
 * applied.
 */
private[hbase] trait HBaseStrategies {
  self: SQLContext#SparkPlanner =>

  private[hbase] object HBaseDataSource extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Aggregate(groupingExpressions, aggregateExpressions, child)
        if groupingExpressions.nonEmpty &&
          canBeAggregatedForAll(groupingExpressions, aggregateExpressions, child) =>
        val withCodeGen = canBeCodeGened(allAggregates(aggregateExpressions)) && codegenEnabled
        if (withCodeGen) execution.Aggregate(
          // In this case, 'partial = true' doesn't mean it is partial, actually, it is not.
          // We made it to true to avoid adding Exchange operation.
          partial = true,
          groupingExpressions,
          aggregateExpressions,
          planLater(child)) :: Nil
        else execution.Aggregate(
          // In this case, 'partial = true' doesn't mean it is partial, actually, it is not.
          // We made it to true to avoid adding Exchange operation.
          partial = true,
          groupingExpressions,
          aggregateExpressions,
          planLater(child)) :: Nil

      case PhysicalOperation(projectList, inPredicates,
      l@LogicalRelation(relation: HBaseRelation)) =>
        pruneFilterProjectHBase(
          l,
          projectList,
          inPredicates,
          (a, f) => relation.buildScan(a, f)) :: Nil

      case _ => Nil
    }

    def canBeCodeGened(aggs: Seq[AggregateExpression]) = !aggs.exists {
      case _: Sum | _: Count | _: Max | _: CombineSetsAndCount => false
      // The generated set implementation is pretty limited ATM.
      case CollectHashSet(exprs) if exprs.size == 1 &&
        Seq(IntegerType, LongType).contains(exprs.head.dataType) => false
      case _ => true
    }

    def allAggregates(exprs: Seq[Expression]) =
      exprs.flatMap(_.collect { case a: AggregateExpression => a})

    /**
     * Determined to do the aggregation for all directly or do with partial aggregation
     */
    protected def canBeAggregatedForAll(groupingExpressions: Seq[Expression],
                                        aggregateExpressions: Seq[NamedExpression],
                                        child: LogicalPlan): Boolean = {
      def findScanNode(physicalChild: SparkPlan): Option[HBaseSQLTableScan] = physicalChild match {
        case chd: HBaseSQLTableScan => Some(chd)
        case chd if chd.children.size != 1 => None
        case chd => findScanNode(chd.children.head)
      }

      /**
       * @param headEnd the HBaseRawType for the end of head partition
       * @param tailStart the HBaseRawType for the start of tail partition
       * @param keysForGroup the remaining key dimension for grouping
       * @return whether these two partitions are distinguished or not in the given dimension
       */
      def distinguishedForGroupKeys(headEnd: HBaseRawType,
                                    tailStart: HBaseRawType,
                                    keysForGroup: Seq[KeyColumn]): Boolean = {
        //Divide raw type into two parts, one is the raw type for current key dimension,
        //the other is the raw type for the key dimensions left
        def divideRawType(rawType: HBaseRawType, key: KeyColumn)
        : (HBaseRawType, HBaseRawType) = key.dataType match {
          case dt: StringType => rawType.splitAt(rawType.indexWhere(_ == 0x00) + 1)
          case dt if dt.defaultSize >= rawType.length => (rawType, Array())
          case dt => rawType.splitAt(dt.defaultSize)
        }

        if (keysForGroup.isEmpty) true
        else {
          val (curKey, keysLeft) = (keysForGroup.head, keysForGroup.tail)
          val (headEndCurKey, headEndKeysLeft) = divideRawType(headEnd, curKey)
          val (tailStartCurKey, tailStartKeysLeft) = divideRawType(tailStart, curKey)

          if (headEndKeysLeft.isEmpty || tailStartKeysLeft.isEmpty) true
          else if (Bytes.compareTo(tailStartCurKey, headEndCurKey) != 0) true
          else if (keysLeft.nonEmpty) distinguishedForGroupKeys(
            headEndKeysLeft, tailStartKeysLeft, keysLeft)
          else if (headEndKeysLeft.forall(_ == 0x00) || tailStartCurKey.forall(_ == 0x00)) true
          else false
        }
      }

      val physicalChild = planLater(child)
      def aggrWithPartial = false
      def aggrForAll = true

      findScanNode(physicalChild) match {
        case None => aggrWithPartial
        case Some(scanNode: HBaseSQLTableScan) =>
          val hbaseRelation = scanNode.relation

          //If there is only one partition in HBase,
          //we don't need to do the partial aggregation
          if (hbaseRelation.partitions.size == 1) aggrForAll
          else {
            val keysForGroup = hbaseRelation.keyColumns.takeWhile(key =>
              groupingExpressions.exists {
                case expr: AttributeReference => expr.name == key.sqlName
                case _ => false
              })

            //If there exists some expressions in groupingExpressions are not keys
            //or it missed some mid dimensions in the rowkey,
            //that means we have to do it with the partial aggregation.
            //
            //If the groupingExpressions are composed by all keys,
            //that means it need to be grouped by rowkey in all dimensions,
            //so we could do the aggregation for all directly.
            if (keysForGroup.size != groupingExpressions.size) aggrWithPartial
            else if (keysForGroup.size == hbaseRelation.keyColumns.size) aggrForAll
            else {
              val partitionsAfterFilter = scanNode.result.partitions
              val eachPartitionApart = (0 to partitionsAfterFilter.length - 2).forall { case i =>
                val headEnd = partitionsAfterFilter(i).asInstanceOf[HBasePartition]
                  .end.get.asInstanceOf[HBaseRawType]
                val tailStart = partitionsAfterFilter(i + 1).asInstanceOf[HBasePartition]
                  .start.get.asInstanceOf[HBaseRawType]
                //If there exists any two partition are not distinguished from each other
                // for the given rowkey dimensions, we could not do the aggregation for all.
                distinguishedForGroupKeys(headEnd, tailStart, keysForGroup)
              }
              if (eachPartitionApart) aggrForAll
              else aggrWithPartial
            }
          }
      }
    }

    // Based on Catalyst expressions.
    // Almost identical to pruneFilterProjectRaw
    protected def pruneFilterProjectHBase(relation: LogicalRelation,
                                          projectList: Seq[NamedExpression],
                                          filterPredicates: Seq[Expression],
                                          scanBuilder:
                                          (Seq[Attribute], Seq[Expression]) => RDD[InternalRow]) = {

      val projectSet = AttributeSet(projectList.flatMap(_.references))
      val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

      val pushedFilters = if (filterPredicates.nonEmpty) {
        Seq(filterPredicates.map {
          _ transform {
            // Match original case of attributes.
            case a: AttributeReference => relation.attributeMap(a)
            // We will do HBase-specific predicate pushdown so just use the original predicate here
          }
        }.reduceLeft(And))
      } else {
        filterPredicates
      }

      val hbaseRelation = relation.relation.asInstanceOf[HBaseRelation]
      if (projectList.map(_.toAttribute) == projectList &&
        projectSet.size == projectList.size &&
        filterSet.subsetOf(projectSet)) {
        // When it is possible to just use column pruning to get the right projection and
        // when the columns of this projection are enough to evaluate all filter conditions,
        // just do a scan followed by a filter, with no extra project.
        val requestedColumns =
          projectList.asInstanceOf[Seq[Attribute]] // Safe due to if above.
            .map(relation.attributeMap) // Match original case of attributes.

        // We have to use a HBase-specific scanner here while maintain as much compatibility
        // with the data source API as possible, primarily because
        // 1) We need to set up the outputPartitioning field to HBase-specific partitions
        // 2) Future use of HBase co-processor
        // 3) We will do partition-specific predicate pushdown
        // The above two *now* are absent from the PhysicalRDD class.

        HBaseSQLTableScan(hbaseRelation, projectList.map(_.toAttribute),
          scanBuilder(requestedColumns, pushedFilters))
      } else {
        val requestedColumns = projectSet.map(relation.attributeMap).toSeq
        val scan = HBaseSQLTableScan(hbaseRelation, requestedColumns,
          scanBuilder(requestedColumns, pushedFilters))
        Project(projectList, scan)
      }
    }
  }

}
