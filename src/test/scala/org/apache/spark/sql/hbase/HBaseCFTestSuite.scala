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

import org.apache.hadoop.hbase._

/**
 * This is a custom filter test suite running against mini-cluster
 */
class HBaseCFTestSuite extends TestBase {
  private val tableName = "cf"
  private val hbaseTableName = "cf_htable"
  private val hbaseFamilies = Seq("f")

  private val csvPaths = Array("src/test/resources", "sql/hbase/src/test/resources")
  private val csvFile = "cf.txt"
  private val tpath = for (csvPath <- csvPaths if new java.io.File(csvPath).exists()) yield {
    logInfo(s"Following path exists $csvPath\n")
    csvPath
  }
  private[hbase] val csvPath = tpath(0)

  override protected def beforeAll() = {
    /**
     * create hbase table if it does not exists
     */
    super.beforeAll()
    if (!TestHbase.catalog.tableExists(Seq(hbaseTableName))) {
      val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
      hbaseFamilies.foreach { f => descriptor.addFamily(new HColumnDescriptor(f))}
      try {
        TestHbase.catalog.admin.createTable(descriptor)
      } catch {
        case e: TableExistsException =>
          logError(s"Table already exists $tableName", e)
      } finally {
        TestHbase.catalog.stopAdmin()
      }
    }

    /**
     * drop the existing logical table if it exists
     */
    if (TestHbase.catalog.tableExists(Seq(tableName))) {
      val dropSql = "DROP TABLE " + tableName
      try {
        runSql(dropSql)
      } catch {
        case e: IllegalStateException =>
          logError(s"Error occurs while dropping the table $tableName", e)
      }
    }

    /**
     * create table
     */
    val createSql =
      s"""CREATE TABLE cf(
        k1 INTEGER,
        k2 INTEGER,
        k3 INTEGER,
        nk1 INTEGER,
        nk2 INTEGER,
        PRIMARY KEY(k1, k2, k3))
        MAPPED BY
        (cf_htable, COLS=[
          nk1=f.nk1,
          nk2=f.nk2
        ])""".stripMargin

    try {
      runSql(createSql)
    } catch {
      case e: IllegalStateException =>
        logError(s"Error occurs while creating the table $tableName", e)
    }

    /**
     * load the data
     */
    val loadSql = "LOAD DATA LOCAL INPATH '" + s"$csvPath/$csvFile" +
      s"' INTO TABLE $tableName"
    try {
      runSql(loadSql)
    } catch {
      case e: IllegalStateException =>
        logError(s"Error occurs while loading the data $tableName", e)
    }
  }

  override protected def afterAll() = {
    runSql(s"DROP TABLE $tableName")
    super.afterAll()
  }

  test("Query 0") {
    val sql = "SELECT * FROM cf"
    val rows = runSql(sql)
    assert(rows.length == 27)
  }

  test("Query 1") {
    val sql = "SELECT * FROM cf WHERE k1 = 1 OR k1 = 10 OR k1 = 20"
    val rows = runSql(sql)
    assert(rows.length == 3)
  }

  test("Query 2") {
    val sql = "SELECT * FROM cf WHERE k1 < 2 OR k1 = 10 OR k1 > 20"
    val rows = runSql(sql)
    assert(rows.length == 9)
  }

  test("Query 3") {
    val sql = "SELECT * FROM cf WHERE (k1 = 1 OR k1 = 10 OR k1 = 20) AND (k2 = 101 OR k2 = 110 OR k2 = 120) AND (k3 = 1001 OR k3 = 1010 OR k3 = 1020)"
    val rows = runSql(sql)
    assert(rows.length == 3)
  }

  test("Query 4") {
    val sql = "SELECT * FROM cf WHERE (k2 = 101 OR k2 = 110 OR k2 = 120) AND (k3 = 1001 OR k3 = 1010 OR k3 = 1020)"
    val rows = runSql(sql)
    assert(rows.length == 3)
  }

  test("Query 5") {
    val sql = "SELECT * FROM cf WHERE (k3 = 1001 OR k3 = 1010 OR k3 = 1020)"
    val rows = runSql(sql)
    assert(rows.length == 3)
  }

  test("Query 6") {
    val sql = "SELECT * FROM cf WHERE (nk1 = -1 OR nk1 = -10)"
    val rows = runSql(sql)
    assert(rows.length == 2)
  }

  test("Query 7") {
    val sql = "SELECT * FROM cf WHERE (nk2 = -101 OR nk2 = -110)"
    val rows = runSql(sql)
    assert(rows.length == 2)
  }

  test("Query 8") {
    val sql = "SELECT * FROM cf WHERE k1 = 10 AND k2 = 110 AND (k3 = 1001 OR k3 = 1010 OR k3 = 1020)"
    val rows = runSql(sql)
    assert(rows.length == 1)
  }

  test("Query 9") {
    val sql = "SELECT * FROM cf WHERE k1 = 10 AND k2 = 110 AND k3 = 1010"
    val rows = runSql(sql)
    assert(rows.length == 1)
  }

  test("Query 10") {
    val sql = "SELECT * FROM cf WHERE k1 = 10 AND k2 = 110 AND k3 = 1010 AND (nk2 = -101 OR nk2 = -110)"
    val rows = runSql(sql)
    assert(rows.length == 1)
  }
}
