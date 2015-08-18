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

package org.apache.spark.sql.hbase.catalyst.logical

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, LogicalPlan}
@DeveloperApi
case class UpdateTable(
    tableName: String,
    columnsToUpdate: Seq[Attribute],
    values: Seq[String],
    child: LogicalPlan) extends UnaryNode {

  override lazy val resolved: Boolean = columnsToUpdate.forall(_.resolved) && childrenResolved

  override def output: Seq[Attribute] = Seq.empty

}

@DeveloperApi
case class DeleteFromTable(tableName: String, child: LogicalPlan) extends UnaryNode {

  override lazy val resolved: Boolean = childrenResolved

  override def output: Seq[Attribute] = Seq.empty

}