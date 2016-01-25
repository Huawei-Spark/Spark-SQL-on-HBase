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

import org.apache.hadoop.hbase.TableName
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.Exchange
import org.apache.spark.sql.hbase.util.HBaseKVHelper
import org.apache.spark.sql.types._

class HBaseCoproCogenSuite extends TestBase {

  override protected def beforeAll() = {
    super.beforeAll()
    TestHbase.start
    createTableTestblk(useCoprocessor = false)
  }

  override protected def afterAll() = {
    dropTableTestblk
    TestHbase.stop
    super.afterAll()
  }

  def createTableTestblk(useCoprocessor: Boolean = true) = {
    val types = Seq(IntegerType, StringType, IntegerType)

    def generateRowKey(keys: Array[Any], length: Int = -1) = {
      val completeRowKey = HBaseKVHelper.makeRowKey(new GenericRow(keys), types)
      if (length < 0) completeRowKey
      else completeRowKey.take(length)
    }

    val splitKeys: Array[HBaseRawType] = Array(
      generateRowKey(Array(1024, UTF8String("0b"), 0), 3),
      generateRowKey(Array(2048, UTF8String("cc"), 1024), 4),
      generateRowKey(Array(4096, UTF8String("0a"), 0), 4),
      generateRowKey(Array(4096, UTF8String("0b"), 1024), 7),
      generateRowKey(Array(4096, UTF8String("cc"), 0), 7),
      generateRowKey(Array(4096, UTF8String("cc"), 1000))
    )
    if (TestHbase.hsc.catalog.tableExists(Seq("presplit_table")))
    {
      dropNativeHbaseTable("presplit_table")
    }
    TestHbase.hsc.catalog.createHBaseUserTable(TableName.valueOf("presplit_table"), Set("cf"), splitKeys, useCoprocessor)

    val sql =
      s"""CREATE TABLE testblk(col1 INT, col2 STRING, col3 INT, col4 STRING,
          PRIMARY KEY(col1, col2, col3))
          MAPPED BY (presplit_table, COLS=[col4=cf.a])"""
        .stripMargin
    runSql(sql)

    val inputFile = "'" + hbaseHome + "/splitLoadData1.txt'"

    // then load parall data into table
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE testblk"
    runSql(loadSql)
    TestHbase.hsc.catalog.stopAdmin()
  }

  def dropTableTestblk() = {
    runSql("drop table testblk")
    dropNativeHbaseTable("presplit_table")
  }

  test("group test for presplit table without coprocessor but with codegen")
  {
    val originalValue = TestHbase.hsc.conf.codegenEnabled
    TestHbase.hsc.setConf(SQLConf.CODEGEN_ENABLED, "true")
    aggregationTest
    TestHbase.hsc.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  test("group test for presplit table without coprocessor and codegen") {
    val originalValue = TestHbase.hsc.conf.codegenEnabled
    TestHbase.hsc.setConf(SQLConf.CODEGEN_ENABLED, "false")
    aggregationTest
    TestHbase.hsc.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  test("group test for presplit table with coprocessor but without codegen") {
    TestHbase.hsc.catalog.enableCoprocessor(TableName.valueOf("presplit_table"))
    val originalValue = TestHbase.hsc.conf.codegenEnabled
    TestHbase.hsc.setConf(SQLConf.CODEGEN_ENABLED, "false")
    aggregationTest
    TestHbase.hsc.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  test("group test for presplit table with codegen and coprocessor") {
    val originalValue = TestHbase.hsc.conf.codegenEnabled
    TestHbase.hsc.setConf(SQLConf.CODEGEN_ENABLED, "true")
    aggregationTest
    TestHbase.hsc.setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  def aggregationTest = {
    var sql = "select col1,col3 from testblk where col1 < 4096 group by col3,col1"
    checkResult(TestHbase.hsc.sql(sql), containExchange = true, 3)

    sql = "select col1,col2,col3 from testblk group by col1,col2,col3"
    checkResult(TestHbase.hsc.sql(sql), containExchange = false, 7)

    sql = "select col1,col2,col3,count(*) from testblk group by col1,col2,col3,col1+col3"
    checkResult(TestHbase.hsc.sql(sql), containExchange = false, 7) //Sprecial case

    sql = "select count(*) from testblk group by col1+col3"
    checkResult(TestHbase.hsc.sql(sql), containExchange = true, 5)

    sql = "select col1,col2 from testblk where col1 < 4096 group by col1,col2"
    checkResult(TestHbase.hsc.sql(sql), containExchange = false, 3)

    sql = "select col1,col2 from testblk where (col1 = 4096 and col2 < 'cc') group by col1,col2"
    checkResult(TestHbase.hsc.sql(sql), containExchange = true, 2)

    sql = "select col1 from testblk where col1 < 4096 group by col1"
    checkResult(TestHbase.hsc.sql(sql), containExchange = false, 3)

    val result = runSql("select avg(col3) from testblk where col1 < 4096 group by col1")
    assert(result.length == 3)
    val exparr = Array(Array(1024.0), Array(0.0), Array(1024.0))

    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

  }

  def checkResult(df: DataFrame, containExchange: Boolean, size: Int) = {
    df.queryExecution.executedPlan match {
      case a: org.apache.spark.sql.execution.Aggregate =>
        assert(a.child.isInstanceOf[Exchange] == containExchange)
      case a: org.apache.spark.sql.execution.GeneratedAggregate =>
        assert(a.child.isInstanceOf[Exchange] == containExchange)
      case _ => Nil
    }
    assert(df.collect().length == size)
  }
}
