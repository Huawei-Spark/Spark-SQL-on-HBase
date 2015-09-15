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

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.SQLConf.SQLConfEntry

object HBaseSQLConf {
  val PARTITION_EXPIRATION = "spark.sql.hbase.partition.expiration"
  val SCANNER_FETCH_SIZE = "spark.sql.hbase.scanner.fetchsize"
  val USE_COPROCESSOR = "spark.sql.hbase.coprocessor"
  val USE_CUSTOMFILTER = "spark.sql.hbase.customfilter"
}

/**
 * A trait that enables the setting and getting of mutable config parameters/hints.
 *
 */
private[hbase] class HBaseSQLConf extends SQLConf {

  private[spark] override def dialect: String = getConf(SQLConf.DIALECT,
    classOf[HBaseSQLDialect].getCanonicalName)

  import HBaseSQLConf._

  /** The expiration of cached partition (i.e., region) info; defaults to 10 minutes . */
  private[hbase] def partitionExpiration: Long = getConf(
    SQLConfEntry.longConf(PARTITION_EXPIRATION, Some(600L)))
  private[hbase] def scannerFetchSize: Int = getConf(
    SQLConfEntry.intConf(SCANNER_FETCH_SIZE, Some(100)))
  private[hbase] def useCoprocessor: Boolean = getConf(
    SQLConfEntry.booleanConf(USE_COPROCESSOR, Some(true)))
  private[hbase] def useCustomFilter: Boolean = getConf(
    SQLConfEntry.booleanConf(USE_CUSTOMFILTER, Some(true)))
}
