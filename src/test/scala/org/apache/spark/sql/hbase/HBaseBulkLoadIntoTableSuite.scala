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

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hbase.execution._
import org.apache.spark.sql.hbase.util.BinaryBytesUtils
import org.apache.spark.sql.types._

class HBaseBulkLoadIntoTableSuite extends TestBase {
  // Test if we can parse 'LOAD DATA LOCAL INPATH './usr/file.txt' INTO TABLE tb1'
  test("bulk load parser test, local file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD PARALL DATA LOCAL INPATH './usr/file.txt' INTO TABLE tb1"

    val plan: LogicalPlan = parser.parse(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[BulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[BulkLoadIntoTableCommand]
    assert(l.inputPath.equals(raw"./usr/file.txt"))
    assert(l.isLocal)
    assert(l.tableName.equals("tb1"))
  }

  // Test if we can parse 'LOAD DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1'
  test("bulkload parser test, load hdfs file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD PARALL DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1"

    val plan: LogicalPlan = parser.parse(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[BulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[BulkLoadIntoTableCommand]
    assert(l.inputPath.equals(raw"/usr/hdfsfile.txt"))
    assert(!l.isLocal)
    assert(l.tableName.equals("tb1"))
  }

  test("bulkload parser test, using delimiter") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD PARALL DATA INPATH '/usr/hdfsfile.txt' INTO TABLE tb1 FIELDS TERMINATED BY '\\|' "

    val plan: LogicalPlan = parser.parse(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[BulkLoadIntoTableCommand])

    val l = plan.asInstanceOf[BulkLoadIntoTableCommand]
    assert(l.inputPath.equals(raw"/usr/hdfsfile.txt"))
    assert(!l.isLocal)
    assert(l.tableName.equals("tb1"))
    assert(l.delimiter.get.equals(raw"\\|"))
  }

  test("load data into hbase") {

    val drop = "drop table testblk"
    val executeSql0 = TestHbase.executeSql(drop)
    try {
      executeSql0.toRdd.collect()
    } catch {
      case e: IllegalStateException =>
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (testblkHTable, COLS=[col2=cf1.a, col3=cf1.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadData.txt'"

    // then load data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    checkAnswer(TestHbase.sql("select * from testblk"),
      Row("row4", "4", "8") ::
        Row("row5", "5", "10") ::
        Row("row6", "6", "12") :: Nil)

    // cleanup
    runSql(drop)
  }

  test("load parall data into hbase") {

    val drop = "drop table testblk"
    val executeSql0 = TestHbase.executeSql(drop)
    try {
      executeSql0.toRdd.collect()
    } catch {
      case e: IllegalStateException =>
        // do not throw exception here
        println(e.getMessage)
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (testblkHTable, COLS=[col2=cf1.a, col3=cf1.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    checkAnswer(TestHbase.sql("select * from testblk"),
      Row("row4", "4", "8") ::
        Row("row5", "5", "10") ::
        Row("row6", "6", "12") :: Nil)

    // cleanup
    runSql(drop)
  }

  test("load data with null column values into hbase") {

    val drop = "drop table testNullColumnBulkload"
    val executeSql0 = TestHbase.executeSql(drop)
    try {
      executeSql0.toRdd.collect()
    } catch {
      case e: IllegalStateException =>
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testNullColumnBulkload(col1 STRING, col2 STRING, col3 STRING, col4 STRING, PRIMARY KEY(col1))
          MAPPED BY (testNullColumnBulkloadHTable, COLS=[col2=cf1.a, col3=cf1.b, col4=cf1.c])"""
        .stripMargin

    val sql2 =
      s"""select * from testNullColumnBulkload"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadNullableData.txt'"

    // then load data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testNullColumnBulkload"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    val sqlResult = TestHbase.sql("select * from testNullColumnBulkload")
    val rows = sqlResult.collect()
    assert(rows.length == 4, s"load parall data with null column values into hbase")
    assert(rows(0)(1) == null, s"load parall data into hbase test failed to select empty-string col1 value")
    assert(rows(1)(2) == null, s"load parall data into hbase test failed to select empty-string col2 value")
    assert(rows(2)(3) == null, s"load parall data into hbase test failed to select null col3 value")
    checkAnswer(sqlResult,
      Row("row1", null, "8", "101") ::
        Row("row2", "2", null, "102") ::
        Row("row3", "3", "10", null) ::
        Row("row4", null, null, null) :: Nil)

    // cleanup
    runSql(drop)
  }

  test("load parall data with null column values into hbase") {

    val drop = "drop table testNullColumnBulkload"
    val executeSql0 = TestHbase.executeSql(drop)
    try {
      executeSql0.toRdd.collect()
    } catch {
      case e: IllegalStateException =>
        // do not throw exception here
        println(e.getMessage)
    }

    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testNullColumnBulkload(col1 STRING, col2 STRING, col3 STRING, col4 STRING, PRIMARY KEY(col1))
          MAPPED BY (testNullColumnBulkloadHTable, COLS=[col2=cf1.a, col3=cf1.b, col4=cf1.c])"""
        .stripMargin

    val sql2 =
      s"""select * from testNullColumnBulkload"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadNullableData.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testNullColumnBulkload"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    val sqlResult = TestHbase.sql("select * from testNullColumnBulkload")
    val rows = sqlResult.collect()
    assert(rows.length == 4, s"load parall data with null column values into hbase")
    assert(rows(0)(1) == null, s"load parall data into hbase test failed to select empty-string col1 value")
    assert(rows(1)(2) == null, s"load parall data into hbase test failed to select empty-string col2 value")
    assert(rows(2)(3) == null, s"load parall data into hbase test failed to select null col3 value")
    checkAnswer(sqlResult,
      Row("row1", null, "8", "101") ::
        Row("row2", "2", null, "102") ::
        Row("row3", "3", "10", null) ::
        Row("row4", null, null, null) :: Nil)

    // cleanup
    runSql(drop)
  }

  test("load data on hbase table with more than 1 column family") {
    createNativeHbaseTable("multi_cf_table", Seq("cf1", "cf2"))
    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (multi_cf_table, COLS=[col2=cf1.a, col3=cf2.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    checkAnswer(TestHbase.sql("select * from testblk"),
      Row("row4", "4", "8") ::
        Row("row5", "5", "10") ::
        Row("row6", "6", "12") :: Nil)

    // cleanup
    runSql("drop table testblk")
    dropNativeHbaseTable("multi_cf_table")

  }

  test("load parall data on hbase table with more than 1 column family") {
    createNativeHbaseTable("multi_cf_table", Seq("cf1", "cf2"))
    // create sql table map with hbase table and run simple sql
    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (multi_cf_table, COLS=[col2=cf1.a, col3=cf2.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/loadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    checkAnswer(TestHbase.sql("select * from testblk"),
      Row("row4", "4", "8") ::
        Row("row5", "5", "10") ::
        Row("row6", "6", "12") :: Nil)

    // cleanup
    runSql("drop table testblk")
    dropNativeHbaseTable("multi_cf_table")
  }

  test("bulk load for presplit table") {
    val splitKeys = Seq(4, 8, 12).map { x =>
      BinaryBytesUtils.create(IntegerType).toBytes(x)
    }
    TestHbase.catalog.createHBaseUserTable("presplit_table", Set("cf1", "cf2"), splitKeys.toArray)

    val sql1 =
      s"""CREATE TABLE testblk(col1 INT, col2 INT, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (presplit_table, COLS=[col2=cf1.a, col3=cf2.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/splitLoadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    assert(TestHbase.sql("select * from testblk").collect().length == 16)

    // cleanup
    runSql("drop table testblk")
    dropNativeHbaseTable("presplit_table")
  }

  test("parall bulk load for presplit table") {
    val splitKeys = Seq(4, 8, 12).map { x =>
      BinaryBytesUtils.create(IntegerType).toBytes(x)
    }
    TestHbase.catalog.createHBaseUserTable("presplit_table", Set("cf1", "cf2"), splitKeys.toArray)

    val sql1 =
      s"""CREATE TABLE testblk(col1 INT, col2 INT, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (presplit_table, COLS=[col2=cf1.a, col3=cf2.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/splitLoadData.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    assert(runSql("select * from testblk").length == 16)

    // cleanup
    runSql("drop table testblk")
    dropNativeHbaseTable("presplit_table")
  }

  test("parall bulk load presplit table with more than 128 regions") {
    // HBasePartitioner binarySearch throws NPE if # regions > 128
    // commit dae6546373a14d4ceb22680954c3482ed33e346a

    // Create > 130 split keys to generate 131 regions.
    val splitKeys = Seq.range(1, 260, 2).map { x =>
      BinaryBytesUtils.create(IntegerType).toBytes(x)
    }

    TestHbase.catalog.createHBaseUserTable(
      "REGION_CNT_131_HTBL",
      Set("f"),
      splitKeys.toArray)

    val sql1 =
      s"""CREATE TABLE region_cnt_131(col1 INT, col2 INT, PRIMARY KEY(col1))
          MAPPED BY (REGION_CNT_131_HTBL, COLS=[col2=f.a])"""
        .stripMargin

    val regionInfoList =
      TestHbase.hbaseAdmin.getTableRegions(Bytes.toBytes("REGION_CNT_131_HTBL"))
    assert(regionInfoList.size == 131, "Incorrect number of hbase tbl regions.")

    val sql2 =
      s"""select * from region_cnt_131 limit 5"""
        .stripMargin

    val executeSql1 = TestHbase.executeSql(sql1)
    executeSql1.toRdd.collect()

    val executeSql2 = TestHbase.executeSql(sql2)
    executeSql2.toRdd.collect()

    val inputFile = "'" + hbaseHome + "/131_regions.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE region_cnt_131"

    val executeSql3 = TestHbase.executeSql(loadSql)
    executeSql3.toRdd.collect()

    assert(runSql("select * from region_cnt_131").length == 260)

    // cleanup
    runSql("drop table region_cnt_131")
    dropNativeHbaseTable("REGION_CNT_131_HTBL")
  }
}
