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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hbase._

private[hbase] case class AddCoprocessor(sqlContext: SQLContext) extends Rule[SparkPlan] {
  private lazy val catalog = sqlContext.asInstanceOf[HBaseSQLContext].catalog

  private def coprocessorIsAvailable(relation: HBaseRelation): Boolean = {
    catalog.deploySuccessfully.get && catalog.hasCoprocessor(relation.hbaseTableName)
  }

  private def generateNewSubplan(origPlan: SparkPlan): SparkPlan = {
    // reduce project (network transfer) if projection list has duplicate
    val (output, distinctOutput) = (origPlan.output, origPlan.output.distinct)
    val needToReduce: Boolean = distinctOutput.size < output.size
    val subplan: SparkPlan = {
      if (needToReduce) Project(distinctOutput, origPlan) else origPlan
    }

    // If any current directory of region server is not accessible,
    // we could not use codegen, or else it will lead to crashing the HBase region server!!!
    // For details, please read the comment in CheckDirEndPointImpl.
    var oldScan: HBaseSQLTableScan = null
    lazy val codegenEnabled = catalog.pwdIsAccessible && oldScan.codegenEnabled
    val newSubplan = subplan.transformUp {
      case subplanScan: HBaseSQLTableScan =>
        oldScan = subplanScan
        val rdd = new HBaseCoprocessorSQLReaderRDD(
          null, codegenEnabled, oldScan.output, None, sqlContext)
        HBaseSQLTableScan(oldScan.relation, oldScan.output, rdd)
    }

    val oldRDD: HBaseSQLReaderRDD = oldScan.result.asInstanceOf[HBaseSQLReaderRDD]
    val newRDD = new HBaseSQLReaderRDD(
      oldRDD.relation, codegenEnabled,
      oldRDD.useCustomFilter,
      oldRDD.output, Some(newSubplan), new DummyRDD(sqlContext),
      oldRDD.deploySuccessfully,
      oldRDD.filterPred, sqlContext)
    val newScan = new HBaseSQLTableScan(oldRDD.relation, subplan.output, newRDD)

    // add project spark plan if projection list has duplicate
    if (needToReduce) Project(output, newScan) else newScan
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (!sqlContext.conf.asInstanceOf[HBaseSQLConf].useCoprocessor) {
      return plan
    }
    var needToCreateSubplanSeq: Seq[Boolean] = Seq()
    def needToCreateSubplan = needToCreateSubplanSeq.nonEmpty && needToCreateSubplanSeq.last
    plan match {
      // If the plan is tableScan directly, we don't need to use coprocessor
      case HBaseSQLTableScan(_, _, _) => plan
      case Filter(_, child: HBaseSQLTableScan) => plan
      case _ =>
        val result = plan.transformUp {
          case scan: HBaseSQLTableScan if coprocessorIsAvailable(scan.relation) =>
            needToCreateSubplanSeq :+= true
            scan

          case scan: LeafNode =>
            needToCreateSubplanSeq :+= false
            scan

          // If subplan is needed then we need coprocessor plans for both children
          case node: SparkPlan if (node.children.size > 1) &&
            needToCreateSubplanSeq.contains(true) =>
            val newChildren = needToCreateSubplanSeq.zip(node.children).map {
              case (ntcsp, child) =>
                if (ntcsp) generateNewSubplan(child)
                else child
            }
            needToCreateSubplanSeq = Seq()
            node.withNewChildren(newChildren)

          // Since the following two plans using shuffledRDD,
          // we could not pass them to the coprocessor.
          // Thus, their child are used as the subplan for coprocessor processing.
          case exchange: Exchange if needToCreateSubplan =>
            needToCreateSubplanSeq = needToCreateSubplanSeq.init :+ false
            val newPlan = generateNewSubplan(exchange.child)
            exchange.withNewChildren(Seq(newPlan))
          case limit: Limit if needToCreateSubplan =>
            needToCreateSubplanSeq = needToCreateSubplanSeq.init :+ false
            val newPlan = generateNewSubplan(limit.child)
            limit.withNewChildren(Seq(newPlan))

          // We will ignore the case of TakeOrderedAndProject
          //
          // This SparkPlan is generated by the combination of Limit and global Sort
          //  and it will lead to ParallelCollectionRDD after executing.
          // For the computing in ParallelCollectionRDD, it needs ParallelCollectionPartition,
          // which we don't know how to transform from ourHBasePartition.
          case takeOrdered: TakeOrderedAndProject if needToCreateSubplan =>
            needToCreateSubplanSeq = needToCreateSubplanSeq.init :+ false
            takeOrdered
        }
        // Use coprocessor even without shuffling
        if (needToCreateSubplan) generateNewSubplan(result) else result
    }
  }
}
