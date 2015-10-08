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

  val PARTITION_EXPIRATION = SQLConfEntry.longConf("spark.sql.hbase.partition.expiration",
    defaultValue = Some(600L))

  val SCANNER_FETCH_SIZE = SQLConfEntry.intConf("spark.sql.hbase.scanner.fetchsize",
    defaultValue = Some(100))

  val USE_COPROCESSOR = SQLConfEntry.booleanConf("spark.sql.hbase.coprocessor",
    defaultValue = Some(true))

  val USE_CUSTOMFILTER = SQLConfEntry.booleanConf("spark.sql.hbase.customfilter",
    defaultValue = Some(true))
}

/**
 * A trait that enables the setting and getting of mutable config parameters/hints.
 *
 */
private[hbase] class HBaseSQLConf extends SQLConf {

  private[spark] override def dialect: String = getConf(SQLConf.DIALECT,
    classOf[HBaseSQLDialect].getCanonicalName)

  import org.apache.spark.sql.hbase.HBaseSQLConf._

  /** The expiration of cached partition (i.e., region) info; defaults to 10 minutes . */
  private[hbase] def partitionExpiration: Long = getConf(PARTITION_EXPIRATION)

  private[hbase] def scannerFetchSize: Int = getConf(SCANNER_FETCH_SIZE)

  private[hbase] def useCoprocessor: Boolean = getConf(USE_COPROCESSOR)

  private[hbase] def useCustomFilter: Boolean = getConf(USE_CUSTOMFILTER)
}
