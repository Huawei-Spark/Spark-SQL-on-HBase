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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{execution, SQLContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.hbase.HBaseRelation
import org.apache.spark.sql.hbase.util.DataTypeUtils
import org.apache.spark.sql.sources.LogicalRelation

@DeveloperApi
case class UpdateTable(
    tableName: String,
    columnsToUpdate: Seq[Attribute],
    values: Seq[String],
    child: SparkPlan) extends execution.UnaryNode {

  override def output: Seq[Attribute] = Seq.empty

  protected override def doExecute(): RDD[Row] = attachTree(this, "execute") {
    val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]

    val typesValues = values.zip(columnsToUpdate.map(_.dataType)).map { v =>
      DataTypeUtils.string2TypeData(v._1, v._2)
    }
    val input = child.output
    val mutableRow = new SpecificMutableRow(input.map(_.dataType))
    val ordinals = columnsToUpdate.map { att =>
      BindReferences.bindReference(att, input)
    }.map(_.asInstanceOf[BoundReference].ordinal)

    val resRdd = child.execute().mapPartitions { iter =>
      val len = input.length
      iter.map { row =>
        var i = 0
        while (i < len) {
          mutableRow.update(i, row(i))
          i += 1
        }
        ordinals.zip(typesValues).map { x => mutableRow.update(x._1, x._2) }
        mutableRow: Row
      }
    }
    val inputValuesDF = sqlContext.createDataFrame(resRdd, relation.schema)
    relation.insert(inputValuesDF)
    sqlContext.sparkContext.emptyRDD[Row]
  }

}

@DeveloperApi
case class DeleteFromTable(tableName: String, child: SparkPlan) extends execution.UnaryNode {
  override def output: Seq[Attribute] = Seq.empty

  protected override def doExecute(): RDD[Row] = attachTree(this, "execute") {
    val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]

    val input = child.output
    val inputValuesDF = sqlContext.createDataFrame(child.execute(), relation.schema)
    relation.delete(inputValuesDF)
    sqlContext.sparkContext.emptyRDD[Row]
  }
}
