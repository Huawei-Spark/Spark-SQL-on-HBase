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

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.{EnsureRequirements, EnsureRowFormats, SparkPlan}
import org.apache.spark.sql.hbase.execution.{AddCoprocessor, HBaseStrategies}

class HBaseSQLContext(sc: SparkContext) extends SQLContext(sc) {
  self =>

  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  protected[sql] override lazy val conf: SQLConf = new HBaseSQLConf

  HBaseConfiguration.merge(
    sc.hadoopConfiguration, HBaseConfiguration.create(sc.hadoopConfiguration))

  @transient
  override protected[sql] lazy val catalog: HBaseCatalog =
    new HBaseCatalog(this, sc.hadoopConfiguration) with OverrideCatalog

  experimental.extraStrategies = Seq((new SparkPlanner with HBaseStrategies).HBaseDataSource)

  // Attention! : Update the following value when spark updates.
  @transient
  override protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches = Batch("Add exchange", Once, EnsureRequirements(self)) ::
      Batch("Add row converters firstly", Once, EnsureRowFormats) ::
      Batch("Add coprocessor", Once, AddCoprocessor(self)) ::
      Batch("Add row converters secondly", Once, EnsureRowFormats) ::
      Nil
  }
}
