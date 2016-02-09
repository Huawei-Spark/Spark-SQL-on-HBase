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

import org.apache.hadoop.hbase.{HBaseTestingUtility, MiniHBaseCluster}
import org.apache.spark.{SparkConf, SparkContext}


object TestHbase extends HBaseSQLContext(
  new SparkContext("local[2]", "TestSQLContext", new SparkConf(true)
    .set("spark.hadoop.hbase.zookeeper.quorum", "localhost"))) {

  @transient val testUtil: HBaseTestingUtility =
    new HBaseTestingUtility(sparkContext.hadoopConfiguration)

  testUtil.startMiniZKCluster
  logDebug(s"Spin up hbase minicluster with 1 master, 1 RS, 1 dataNode")

  @transient val cluster: MiniHBaseCluster = testUtil.startMiniHBaseCluster(1, 1)
  logInfo(s"Started HBaseMiniCluster with regions = ${cluster.countServedRegions}")

  logInfo(s"Configuration zkPort="
    + s"${sparkContext.hadoopConfiguration.get("hbase.zookeeper.property.clientPort")}")

  // The following operation will initialize the HBaseCatalog.
  // And it should be done after starting MiniHBaseCluster
  catalog.deploySuccessfully_internal = Some(true)
  catalog.pwdIsAccessible = true

  def hbaseAdmin = catalog.admin
}
