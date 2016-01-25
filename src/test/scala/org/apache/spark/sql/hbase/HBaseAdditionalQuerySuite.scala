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

class HBaseAdditionalQuerySuite extends TestBase {

  override protected def beforeAll() = {
    super.beforeAll()
    TestHbase.start
    createTableTeacher()
    createTablePeople()
    createTableFromParquet()
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
    val outputFile = hbaseHome + "/users.parquet"
    val parquetTable = TestHbase.hsc.read.parquet(outputFile)
    parquetTable.registerTempTable("parquetTable")
  }

  override protected def afterAll() = {
    dropTableTeacher()
    dropTablePeople()
    TestHbase.stop
    super.afterAll()
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
    checkResult(TestHbase.hsc.sql(sql), containExchange = true, 7)
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
    val df: DataFrame = TestHbase.hsc.table("spark_teacher_3key")
    import org.apache.spark.sql.functions._
    assert(df.select(col("*"), randn(5L)).collect().size == 12)
    assert(df.select(rand(), acos("teacher_age")).collect().size == 12)
  }

  test("Union Parquet Table Test") {
    val sql =
      """select * from (
        |select teacher_name from spark_teacher_3key t1
        |union all select name from parquetTable) t3""".stripMargin
    checkResult(TestHbase.hsc.sql(sql), containExchange = true, 15)
  }

  test("Join Parquet Table Table") {
    val sql =
      """select * from spark_teacher_3key
        |join parquetTable where parquetTable.name="Bruce"
        |or parquetTable.favorite_color="Blue" """.stripMargin
    checkResult(TestHbase.hsc.sql(sql), containExchange = true, 24)
  }

  test("NO Coprocessor and No CustomerFilter Test") {
    val origValOfCoprocessor = TestHbase.hsc.conf.getConf(HBaseSQLConf.USE_COPROCESSOR, "true")
    val origValOfCustomfilter = TestHbase.hsc.conf.getConf(HBaseSQLConf.USE_CUSTOMFILTER, "true")

    TestHbase.hsc.setConf(HBaseSQLConf.USE_COPROCESSOR, "false")
    TestHbase.hsc.setConf(HBaseSQLConf.USE_CUSTOMFILTER, "false")

    val r1 = runSql(
      "select grade,class, subject , teacher_name, teacher_age from spark_teacher_3key where grade = 1 or class < 3")
    assert(r1.size == 12)

    val r2 = runSql(
      "select school_identification from spark_people where school_director is null")
    assert(r2.size == 2)

    TestHbase.hsc.setConf(HBaseSQLConf.USE_COPROCESSOR, origValOfCoprocessor)
    TestHbase.hsc.setConf(HBaseSQLConf.USE_CUSTOMFILTER, origValOfCustomfilter)
  }

  test("DataFrame Test") {
    val teachers: DataFrame = TestHbase.hsc.sql("Select * from spark_teacher_3key")
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
    TestHbase.hsc.udf.register("myFilter", myFilter _)
    val result = TestHbase.hsc.sql("Select count(*) from spark_teacher_3key WHERE myFilter(teacher_name)")
    result.foreach(r => require(r.getLong(0) == 3L))
  }

  test("UDF Test with custom filter but without coprocessor") {
    TestHbase.hsc.setConf(HBaseSQLConf.USE_COPROCESSOR, "false")
    def myFilter(s: String) = s contains "_1_2"
    TestHbase.hsc.udf.register("myFilter", myFilter _)
    val result = TestHbase.hsc.sql("Select count(*) from spark_teacher_3key WHERE myFilter(teacher_name)")
    result.foreach(r => require(r.getLong(0) == 3L))
    TestHbase.hsc.setConf(HBaseSQLConf.USE_COPROCESSOR, "true")
  }

  test("UDF Test with coprocessor but without custom filter") {
    TestHbase.hsc.setConf(HBaseSQLConf.USE_CUSTOMFILTER, "false")
    def myFilter(s: String) = s contains "_1_2"
    TestHbase.hsc.udf.register("myFilter", myFilter _)
    val result = TestHbase.hsc.sql("Select count(*) from spark_teacher_3key WHERE myFilter(teacher_name)")
    result.foreach(r => require(r.getLong(0) == 3L))
    TestHbase.hsc.setConf(HBaseSQLConf.USE_CUSTOMFILTER, "true")
  }

  test("UDF Test without coprocessor and custom filter") {
    TestHbase.hsc.setConf(HBaseSQLConf.USE_COPROCESSOR, "false")
    TestHbase.hsc.setConf(HBaseSQLConf.USE_CUSTOMFILTER, "false")
    def myFilter(s: String) = s contains "_1_2"
    TestHbase.hsc.udf.register("myFilter", myFilter _)
    val result = TestHbase.hsc.sql("Select count(*) from spark_teacher_3key WHERE myFilter(teacher_name)")
    result.foreach(r => require(r.getLong(0) == 3L))
    TestHbase.hsc.setConf(HBaseSQLConf.USE_COPROCESSOR, "true")
    TestHbase.hsc.setConf(HBaseSQLConf.USE_CUSTOMFILTER, "true")
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
