package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase._

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

/**
 * CreateTableAndLoadData
 *
 */
class TestBaseWithNonSplitData extends TestBase {
  val TestTableName = "TestTable"
  val TestHBaseTableName: String = s"Hb$TestTableName"
  val TestHbaseColFamilies = Seq("cf1", "cf2")

  val CsvPaths = Array("src/test/resources", "sql/hbase/src/test/resources")
  val DefaultLoadFile = "testTable.txt"

  private val tpath = for (csvPath <- CsvPaths
                           if new java.io.File(csvPath).exists()
  ) yield {
    logInfo(s"Following path exists $csvPath")
    csvPath
  }
  private[hbase] val CsvPath = tpath(0)

  override protected def beforeAll() = {
    super.beforeAll()
    TestHbase.start
    val testTableCreationSQL = s"""CREATE TABLE $TestTableName(strcol STRING, bytecol BYTE,
                               shortcol SHORT, intcol INTEGER,
            longcol LONG, floatcol FLOAT, doublecol DOUBLE, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY ($TestHBaseTableName, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
      .stripMargin
    createTable(TestTableName, TestHBaseTableName, testTableCreationSQL)
    loadData(TestTableName, s"$CsvPath/$DefaultLoadFile")
  }

  override protected def afterAll() = {
    runSql("DROP TABLE " + TestTableName)
    dropNativeHbaseTable(TestHBaseTableName)
    TestHbase.stop
    super.afterAll()
  }

  def createTable(tableName: String, hbaseTable: String, creationSQL: String) = {
    val hbaseAdmin = TestHbase.hbaseAdmin
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseTable))) {
      createNativeHbaseTable(hbaseTable, TestHbaseColFamilies)
    }

    if (TestHbase.hsc.catalog.tableExists(Seq(tableName))) {
      val dropSql = s"DROP TABLE $tableName"
      runSql(dropSql)
    }

    try {
      logInfo(s"invoking $creationSQL ..")
      runSql(creationSQL)
    } catch {
      case e: TableExistsException =>
        logInfo("IF NOT EXISTS still not implemented so we get the following exception", e)
    }
  }
}
