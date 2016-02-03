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

import org.apache.hadoop.hbase.client.{ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseTestingUtility, MiniHBaseCluster}
import org.apache.spark.{SparkConf, SparkContext}


object TestHbase {

  private var hsc_ : HBaseSQLContext = _

  def hsc: HBaseSQLContext = {
    if (hsc_ == null) {
      hsc_ = new HBaseSQLContext(new SparkContext("local", "TestSQLContext", new SparkConf(true)
        .set("spark.hadoop.hbase.zookeeper.quorum", "localhost")))
    }
    hsc_
  }

  @transient var testUtil: HBaseTestingUtility = _

  def start: Unit = {
    // testUtil = new HBaseTestingUtility(hsc.sparkContext.hadoopConfiguration)
    testUtil = new HBaseTestingUtility(hsc.sparkContext.hadoopConfiguration)
    testUtil.startMiniZKCluster
    testUtil.startMiniHBaseCluster(1, 1)
    // The following operation will initialize the HBaseCatalog.
    // And it should be done after starting MiniHBaseCluster
    hsc.catalog.deploySuccessfully_internal = Some(true)
    hsc.catalog.pwdIsAccessible = true
  }

  def stop: Unit = {
    hsc_.catalog.stopAdmin()
    hsc_.catalog.connection.close()
    hsc.sparkContext.stop()
    hsc_ = null
    testUtil.cleanupDataTestDirOnTestFS()
    testUtil.cleanupTestDir()
    testUtil.shutdownMiniCluster()
    testUtil.shutdownMiniZKCluster()
    testUtil = null
  }

  /*
  hsc.logInfo(s"Configuration zkPort="
    + s"${hsc.sparkContext.hadoopConfiguration.get("hbase.zookeeper.property.clientPort")}")
*/

  def hbaseAdmin = hsc.catalog.admin
}