
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

import java.io.File
import java.util.Date

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableExistsException, TableName}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Suite}

abstract class TestBase
  extends FunSuite with BeforeAndAfterAll with Logging {
  self: Suite =>

  val startTime = (new Date).getTime
  val hbaseHome = {
    val loader = this.getClass.getClassLoader
    val url = loader.getResource("loadData.txt")
    val file = new File(url.getPath)
    val parent = file.getParentFile
    parent.getAbsolutePath
  }
  if (hbaseHome == null || hbaseHome.isEmpty)
    logError("Spark Home is not defined; may lead to unexpected error!")

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param rdd the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result, can either be an Any, Seq[Product], or Seq[ Seq[Any] ].
   */
  protected def checkAnswer(rdd: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val isSorted = rdd.logicalPlan.collect { case s: logical.Sort => s}.nonEmpty
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case o => o
        })
      }
      if (!isSorted) converted.sortBy(_.toString()) else converted
    }
    val sparkAnswer = try rdd.collect().toSeq catch {
      case e: Exception =>
        fail(
          s"""
            |Exception thrown while executing query:
            |${rdd.queryExecution}
            |== Exception ==
            |$e
            |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin)
    }

    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      fail( s"""
        |Results do not match for query:
        |${rdd.logicalPlan}
        |== Analyzed Plan ==
        |${rdd.queryExecution.analyzed}
        |== Physical Plan ==
        |${rdd.queryExecution.executedPlan}
        |== Results ==
        |${
        sideBySide(
          s"== Correct Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
          s"== Spark Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")
      }
      """.stripMargin)
    }
  }

  protected def checkAnswer(rdd: DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(rdd, Seq(expectedAnswer))
  }

  def runSql(sql: String) = {
    logInfo(sql)
    TestHbase.sql(sql).collect()
  }

  override protected def afterAll(): Unit = {
    val msg = s"Test ${getClass.getName} completed at ${(new java.util.Date).toString} duration=${((new java.util.Date).getTime - startTime) / 1000}"
    logInfo(msg)
  }

  val CompareTol = 1e-6

  def compareWithTol(actarr: Seq[Any], exparr: Seq[Any], emsg: String): Boolean = {
    actarr.zip(exparr).forall { case (aa, ee) =>
      val eq = (aa, ee) match {
        case (a: Double, e: Double) =>
          Math.abs(a - e) <= CompareTol
        case (a: Float, e: Float) =>
          Math.abs(a - e) <= CompareTol
        case (a: Byte, e) => true //For now, we assume it is ok
        case (a, e) =>
          if (a == null && e == null) {
            logDebug(s"a=null e=null")
          } else {
            logDebug(s"atype=${a.getClass.getName} etype=${e.getClass.getName}")
          }
          a == e
        case _ => throw new IllegalArgumentException("Expected tuple")
      }
      if (!eq) {
        logError(s"$emsg: Mismatch- act=$aa exp=$ee")
      }
      eq
    }
  }

  def verify(testName: String, sql: String, result1: Seq[Seq[Any]], exparr: Seq[Seq[Any]]) = {
    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result1(rx), exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}

    logInfo(s"$sql came back with ${result1.size} results")
    logInfo(result1.mkString)
    assert(res, "One or more rows did not match expected")
  }

  def createNativeHbaseTable(tableName: String, families: Seq[String]) = {
    val hbaseAdmin = TestHbase.hbaseAdmin
    val hdesc = new HTableDescriptor(TableName.valueOf(tableName))
    families.foreach { f => hdesc.addFamily(new HColumnDescriptor(f))}
    try {
      hbaseAdmin.createTable(hdesc)
    } catch {
      case e: TableExistsException =>
        logError(s"Table already exists $tableName", e)
    }
  }

  def dropNativeHbaseTable(tableName: String) = {
    try {
      val hbaseAdmin = TestHbase.hbaseAdmin
      hbaseAdmin.disableTable(tableName)
      hbaseAdmin.deleteTable(tableName)
    } catch {
      case e: TableExistsException =>
        logError(s"Table already exists $tableName", e)
    }
  }

  def loadData(tableName: String, loadFile: String) = {
    // then load data into table
    val loadSql = s"LOAD PARALL DATA LOCAL INPATH '$loadFile' INTO TABLE $tableName"
    runSql(loadSql)
  }

  def printRows(rows: Array[Row]) = {
    println("======= QUERY RESULTS ======")
    for (i <- rows.indices) {
      println(rows(i).mkString(" | "))
    }
    println("============================")
  }
}
