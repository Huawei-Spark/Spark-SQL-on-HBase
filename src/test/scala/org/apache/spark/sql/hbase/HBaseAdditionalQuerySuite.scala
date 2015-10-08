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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.Exchange
import org.apache.spark.sql.hbase.util.HBaseKVHelper
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types._

class HBaseAdditionalQuerySuite extends TestBase {

  override protected def beforeAll() = {
    createTableTeacher()
    createTablePeople()
    createTableFromParquet()
  }

  def createTableTestblk(useCoprocessor: Boolean = true) = {
    val types = Seq(IntegerType, StringType, IntegerType)

    def generateRowKey(keys: Array[Any], length: Int = -1) = {
      val completeRowKey = HBaseKVHelper.makeRowKey(new GenericInternalRow(keys), types)
      if (length < 0) completeRowKey
      else completeRowKey.take(length)
    }

    val splitKeys: Array[HBaseRawType] = Array(
      generateRowKey(Array(1024, UTF8String.fromString("0b"), 0), 3),
      generateRowKey(Array(2048, UTF8String.fromString("cc"), 1024), 4),
      generateRowKey(Array(4096, UTF8String.fromString("0a"), 0), 4),
      generateRowKey(Array(4096, UTF8String.fromString("0b"), 1024), 7),
      generateRowKey(Array(4096, UTF8String.fromString("cc"), 0), 7),
      generateRowKey(Array(4096, UTF8String.fromString("cc"), 1000))
    )
    TestHbase.catalog.createHBaseUserTable("presplit_table", Set("cf"), splitKeys, useCoprocessor)

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
  }

  def createTableTeacher(useCoprocessor: Boolean = true) = {
    val sql =
      s"""CREATE TABLE spark_teacher_3key(
          grade INT, class INT, subject STRING, teacher_name STRING, teacher_age INT,
          PRIMARY KEY(grade, class, subject))
          MAPPED BY (teacher, COLS=[teacher_name=cf.a, teacher_age=cf.b])"""
        .stripMargin
    runSql(sql)

    val inputFile = "'" + hbaseHome + "/teacher.txt'"
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE spark_teacher_3key"
    runSql(loadSql)
  }

  def createTablePeople(useCoprocessor: Boolean = true) = {
    val sql =
      s"""CREATE TABLE spark_people(
          rowNum INT, people_name STRING, people_age INT,
          school_identification STRING, school_director STRING,
          PRIMARY KEY(rowNum))
          MAPPED BY (people, COLS=[people_name=cf.a, people_age=cf.b,
          school_identification=cf.c, school_director=cf.d])"""
        .stripMargin
    runSql(sql)

    val inputFile = "'" + hbaseHome + "/people.txt'"
    val loadSql = "LOAD PARALL DATA LOCAL INPATH " + inputFile + " INTO TABLE spark_people"
    runSql(loadSql)
  }

  def createTableFromParquet() = {
    val outputFile = "file://" + hbaseHome + "/users.parquet"
    val parquetTable = TestHbase.read.parquet(outputFile)
    parquetTable.registerTempTable("parquetTable")
  }

  override protected def afterAll() = {
    dropTableTeacher()
    dropTablePeople()
  }

  def dropTableTestblk() = {
    runSql("drop table testblk")
    dropNativeHbaseTable("presplit_table")
  }

  def dropTableTeacher() = {
    runSql("drop table spark_teacher_3key")
    dropNativeHbaseTable("teacher")
  }

  def dropTablePeople() = {
    runSql("drop table spark_people")
    dropNativeHbaseTable("people")
  }

  test("UNION TEST") {
    val sql = "select people_name from spark_people union select teacher_name from spark_teacher_3key"
    checkResult(TestHbase.sql(sql), containExchange = true, 7)
  }

  test("SORT TEST") {
    val result = runSql("select teacher_name from spark_teacher_3key order by grade limit 2")
    val exparr = Array(Array("teacher_1_1_1"), Array("teacher_1_2_1"))
    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")
  }

  test("TEST Random") {
    val df: DataFrame = TestHbase.table("spark_teacher_3key")
    import org.apache.spark.sql.functions._
    assert(df.select(col("*"), randn(5L)).collect().size == 12)
    assert(df.select(rand, acos("teacher_age")).collect().size == 12)
  }

  test("Union Parquet Table Test") {
    val sql =
      """select * from (
        |select teacher_name from spark_teacher_3key t1
        |union all select name from parquetTable) t3""".stripMargin
    checkResult(TestHbase.sql(sql), containExchange = true, 15)
  }

  test("Join Parquet Table Table") {
    val sql =
      """select * from spark_teacher_3key
        |join parquetTable where parquetTable.name="Bruce"
        |or parquetTable.favorite_color="Blue" """.stripMargin
    checkResult(TestHbase.sql(sql), containExchange = true, 24)
  }

  test("NO Coprocessor and No CustomerFilter Test") {
    val origValOfCoprocessor = TestHbase.conf.getConf(HBaseSQLConf.USE_COPROCESSOR)
    val origValOfCustomfilter = TestHbase.conf.getConf(HBaseSQLConf.USE_CUSTOMFILTER)

    TestHbase.setConf(HBaseSQLConf.USE_COPROCESSOR, false)
    TestHbase.setConf(HBaseSQLConf.USE_CUSTOMFILTER, false)

    val r1 = runSql(
      "select grade,class, subject , teacher_name, teacher_age from spark_teacher_3key where grade = 1 or class < 3")
    assert(r1.size == 12)

    val r2 = runSql(
      "select school_identification from spark_people where school_director is null")
    assert(r2.size == 2)

    TestHbase.setConf(HBaseSQLConf.USE_COPROCESSOR, origValOfCoprocessor)
    TestHbase.setConf(HBaseSQLConf.USE_CUSTOMFILTER, origValOfCustomfilter)
  }

  test("DataFrame Test") {
    val teachers: DataFrame = TestHbase.sql("Select * from spark_teacher_3key")
    val result = teachers.orderBy(Column("grade").asc, Column("class").asc)
      .select("teacher_name").limit(3).collect()
    result.foreach(println)
    val exparr = Array(Array("teacher_1_1_1"), Array("teacher_1_2_1"), Array("teacher_1_3_1"))
    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")
  }

  test("UDF Test with both coprocessor and custom filter") {
    def myFilter(s: String) = s contains "_1_2"
    TestHbase.udf.register("myFilter", myFilter _)
    val result = TestHbase.sql("Select count(*) from spark_teacher_3key WHERE myFilter(teacher_name)")
    result.foreach(r => require(r.getLong(0) == 3L))
  }

  test("UDF Test with custom filter but without coprocessor") {
    val originalValue = TestHbase.conf.asInstanceOf[HBaseSQLConf].useCustomFilter
    TestHbase.setConf(HBaseSQLConf.USE_COPROCESSOR, false)
    def myFilter(s: String) = s contains "_1_2"
    TestHbase.udf.register("myFilter", myFilter _)
    val result = TestHbase.sql("Select count(*) from spark_teacher_3key WHERE myFilter(teacher_name)")
    result.foreach(r => require(r.getLong(0) == 3L))
    TestHbase.setConf(HBaseSQLConf.USE_COPROCESSOR, originalValue)
  }

  test("UDF Test with coprocessor but without custom filter") {
    val originalValue = TestHbase.conf.asInstanceOf[HBaseSQLConf].useCoprocessor
    TestHbase.setConf(HBaseSQLConf.USE_CUSTOMFILTER, false)
    def myFilter(s: String) = s contains "_1_2"
    TestHbase.udf.register("myFilter", myFilter _)
    val result = TestHbase.sql("Select count(*) from spark_teacher_3key WHERE myFilter(teacher_name)")
    result.foreach(r => require(r.getLong(0) == 3L))
    TestHbase.setConf(HBaseSQLConf.USE_CUSTOMFILTER, originalValue)
  }

  test("UDF Test without coprocessor and custom filter") {
    val originalValueCopro = TestHbase.conf.asInstanceOf[HBaseSQLConf].useCoprocessor
    val originalValueCF = TestHbase.conf.asInstanceOf[HBaseSQLConf].useCustomFilter

    TestHbase.setConf(HBaseSQLConf.USE_COPROCESSOR, false)
    TestHbase.setConf(HBaseSQLConf.USE_CUSTOMFILTER, false)
    def myFilter(s: String) = s contains "_1_2"
    TestHbase.udf.register("myFilter", myFilter _)
    val result = TestHbase.sql("Select count(*) from spark_teacher_3key WHERE myFilter(teacher_name)")
    result.foreach(r => require(r.getLong(0) == 3L))
    TestHbase.setConf(HBaseSQLConf.USE_COPROCESSOR, originalValueCopro)
    TestHbase.setConf(HBaseSQLConf.USE_CUSTOMFILTER, originalValueCF)
  }

  test("group test for presplit table with coprocessor but without codegen") {
    aggregationTest()
  }

  test("group test for presplit table without coprocessor and codegen") {
    aggregationTest(useCoprocessor = false)
  }

  test("group test for presplit table with codegen and coprocessor") {
    val originalValue = TestHbase.conf.codegenEnabled
    TestHbase.setConf(SQLConf.CODEGEN_ENABLED, true)
    aggregationTest()
    TestHbase.setConf(SQLConf.CODEGEN_ENABLED, originalValue)
  }

  test("group test for presplit table with codegen but without coprocessor") {
    val originalValue = TestHbase.conf.codegenEnabled
    TestHbase.setConf(SQLConf.CODEGEN_ENABLED, true)
    aggregationTest(useCoprocessor = false)
    TestHbase.setConf(SQLConf.CODEGEN_ENABLED, originalValue)
  }

  def aggregationTest(useCoprocessor: Boolean = true) = {
    createTableTestblk(useCoprocessor = useCoprocessor)

    var sql = "select col1,col3 from testblk where col1 < 4096 group by col3,col1"
    checkResult(TestHbase.sql(sql), containExchange = true, 3)

    sql = "select col1,col2,col3 from testblk group by col1,col2,col3"
    checkResult(TestHbase.sql(sql), containExchange = false, 7)

    sql = "select col1,col2,col3,count(*) from testblk group by col1,col2,col3,col1+col3"
    checkResult(TestHbase.sql(sql), containExchange = false, 7) //Sprecial case

    sql = "select count(*) from testblk group by col1+col3"
    checkResult(TestHbase.sql(sql), containExchange = true, 5)

    sql = "select col1,col2 from testblk where col1 < 4096 group by col1,col2"
    checkResult(TestHbase.sql(sql), containExchange = false, 3)

    sql = "select col1,col2 from testblk where (col1 = 4096 and col2 < 'cc') group by col1,col2"
    checkResult(TestHbase.sql(sql), containExchange = true, 2)

    sql = "select col1 from testblk where col1 < 4096 group by col1"
    checkResult(TestHbase.sql(sql), containExchange = false, 3)

    val result = runSql("select avg(col3) from testblk where col1 < 4096 group by col1")
    assert(result.length == 3)
    val exparr = Array(Array(1024.0), Array(0.0), Array(1024.0))

    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    dropTableTestblk()
  }

  def checkResult(df: DataFrame, containExchange: Boolean, size: Int) = {
    df.queryExecution.executedPlan match {
      case a: org.apache.spark.sql.execution.Aggregate =>
        assert(a.child.isInstanceOf[Exchange] == containExchange)
      case _ => Nil
    }
    assert(df.collect().length == size)
  }
}
