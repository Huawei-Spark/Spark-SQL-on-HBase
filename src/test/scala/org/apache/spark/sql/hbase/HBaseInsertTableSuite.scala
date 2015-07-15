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

import org.apache.spark.sql.Row

class HBaseInsertTableSuite extends TestBaseWithNonSplitData {

  var testnm = "Insert all rows to the table from other table"
  test("Insert all rows to the table from other table") {
    val createQuery = s"""CREATE TABLE insertTestTable(strcol STRING, bytecol BYTE, shortcol SHORT,
            intcol INTEGER, longcol LONG, floatcol FLOAT, doublecol DOUBLE, 
            PRIMARY KEY(doublecol, strcol, intcol)) 
            MAPPED BY (hinsertTestTable, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
      .stripMargin
    runSql(createQuery)

    val insertQuery =
      s"""INSERT INTO TABLE insertTestTable SELECT * FROM $TestTableName"""
        .stripMargin
    runSql(insertQuery)

    val testQuery = "SELECT * FROM insertTestTable"
    val testResult = runSql(testQuery)
    val targetResult = runSql(s"SELECT * FROM $TestTableName")
    assert(testResult.length == targetResult.length, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    runSql("DROP TABLE insertTestTable")
  }

  testnm = "Insert few rows to the table from other table after applying filter"
  test("Insert few rows to the table from other table after applying filter") {
    val createQuery = s"""CREATE TABLE insertTestTableFilter(strcol STRING, bytecol BYTE,
            shortcol SHORT, intcol INTEGER, longcol LONG, floatcol FLOAT, doublecol DOUBLE, 
            PRIMARY KEY(doublecol, strcol, intcol)) 
            MAPPED BY (hinsertTestTableFilter, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
      .stripMargin
    runSql(createQuery)

    val insertQuery =
      s"""insert into table insertTestTableFilter select * from $TestTableName
        where doublecol > 5678912.345681"""
        .stripMargin
    runSql(insertQuery)

    val testQuery = "select * from insertTestTableFilter"
    val testResult = runSql(testQuery)
    val targetResult = runSql(s"select * from $TestTableName where doublecol > 5678912.345681")
    assert(testResult.length == targetResult.length, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    runSql("Drop Table insertTestTableFilter")
  }

  def compareResults(fetchResult: Array[Row], targetResult: Array[Row]) = {
    val res = {
      for (rx <- targetResult.indices)
      yield compareWithTol(fetchResult(rx).toSeq, targetResult(rx).toSeq, s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")
  }

  testnm = "Insert few columns to the table from other table"
  test("Insert few columns to the table from other table") {
    val createQuery = s"""CREATE TABLE insertTestTableFewCols(strcol STRING, bytecol BYTE,
            shortcol SHORT, intcol INTEGER, PRIMARY KEY(strcol, intcol)) 
            MAPPED BY (hinsertTestTableFewCols, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol])"""
      .stripMargin
    runSql(createQuery)

    val insertQuery =
      s"""INSERT INTO TABLE insertTestTableFewCols SELECT strcol, bytecol,
        shortcol, intcol FROM $TestTableName ORDER BY strcol"""
        .stripMargin
    runSql(insertQuery)

    val testQuery =
      "SELECT strcol, bytecol, shortcol, intcol FROM insertTestTableFewCols ORDER BY strcol"
    val testResult = runSql(testQuery)
    val targetResult =
      runSql(s"SELECT strcol, bytecol, shortcol, intcol FROM $TestTableName ORDER BY strcol")
    assert(testResult.length == targetResult.length, s"$testnm failed on size")

    compareResults(testResult, targetResult)

    runSql("DROP TABLE insertTestTableFewCols")
  }

  testnm = "Insert into values test"
  test("Insert into values test") {
    val createQuery = s"""CREATE TABLE insertValuesTest(strcol STRING, bytecol BYTE,
            shortcol SHORT, intcol INTEGER, PRIMARY KEY(strcol, intcol)) 
            MAPPED BY (hinsertValuesTest, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol])"""
      .stripMargin
    runSql(createQuery)

    val insertQuery1 = s"INSERT INTO TABLE insertValuesTest VALUES('Row0','a',12340,23456780)"
    val insertQuery2 = s"INSERT INTO TABLE insertValuesTest VALUES('Row1','b',12345,23456789)"
    val insertQuery3 = s"INSERT INTO TABLE insertValuesTest VALUES('Row2','c',12342,23456782)"
    runSql(insertQuery1)
    runSql(insertQuery2)
    runSql(insertQuery3)

    val testQuery = "SELECT * FROM insertValuesTest ORDER BY strcol"
    val testResult = runSql(testQuery)
    assert(testResult.length == 3, s"$testnm failed on size")

    val exparr = Array(Array("Row0", 'a', 12340, 23456780),
      Array("Row1", 'b', 12345, 23456789),
      Array("Row2", 'c', 12342, 23456782))

    val res = {
      for (rx <- 0 until 3)
      yield compareWithTol(testResult(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    runSql("DROP TABLE insertValuesTest")
  }

  testnm = "Insert nullable values test"
  test("Insert nullable values test") {
    val createQuery = s"""CREATE TABLE insertNullValuesTest(strcol STRING, bytecol BYTE,
            shortcol SHORT, intcol INTEGER, PRIMARY KEY(strcol))
            MAPPED BY (hinsertNullValuesTest, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, intcol=cf1.hintcol])"""
      .stripMargin
    runSql(createQuery)

    val insertQuery1 = s"INSERT INTO TABLE insertNullValuesTest VALUES('Row0', null,  12340, 23456780)"
    val insertQuery2 = s"INSERT INTO TABLE insertNullValuesTest VALUES('Row1', 'b',   null, 23456789)"
    val insertQuery3 = s"INSERT INTO TABLE insertNullValuesTest VALUES('Row2', 'c',  12342, null)"
    runSql(insertQuery1)
    runSql(insertQuery2)
    runSql(insertQuery3)

    val selectAllQuery = "SELECT * FROM insertNullValuesTest ORDER BY strcol"
    val selectAllResult = runSql(selectAllQuery)

    assert(selectAllResult.length == 3, s"$testnm failed on size")

    var currentResultRow: Int = 0

    // check 1st result row
    assert(selectAllResult(currentResultRow).length == 4, s"$testnm failed on row size (# of cols)")
    assert(selectAllResult(currentResultRow)(0) === s"Row0", s"$testnm failed on returned Row0, key value")
    assert(selectAllResult(currentResultRow)(1) == null, s"$testnm failed on returned Row0, null col1 value")
    assert(selectAllResult(currentResultRow)(2) == 12340, s"$testnm failed on returned Row0, col2 value")
    assert(selectAllResult(currentResultRow)(3) == 23456780, s"$testnm failed on returned Row0, col3 value")

    currentResultRow += 1

    // check 2nd result row
    assert(selectAllResult(currentResultRow)(0) === s"Row1", s"$testnm failed on returned Row1, key value")
    // skip comparison of actual and expected bytecol value
    assert(selectAllResult(currentResultRow)(2) == null, s"$testnm failed on returned Row1, null col2 value")
    assert(selectAllResult(currentResultRow)(3) == 23456789, s"$testnm failed on returned Row1, col3 value")

    currentResultRow += 1

    // check 3rd result row
    assert(selectAllResult(currentResultRow)(0) === s"Row2", s"$testnm failed on returned Row2, key value")
    // skip comparison of actual and expected bytecol value
    assert(selectAllResult(currentResultRow)(2) == 12342, s"$testnm failed on returned Row2, col2 value")
    assert(selectAllResult(currentResultRow)(3) == null, s"$testnm failed on returned Row2, null col3 value")

    // test 'where col is not null'

    val selectWhereIsNotNullQuery = "SELECT * FROM insertNullValuesTest WHERE intcol IS NOT NULL ORDER BY strcol"
    val selectWhereIsNotNullResult = runSql(selectWhereIsNotNullQuery)
    assert(selectWhereIsNotNullResult.length == 2, s"$testnm failed on size")

    currentResultRow = 0
    // check 1st result row
    assert(selectWhereIsNotNullResult(currentResultRow)(0) === s"Row0", s"$testnm failed on returned Row0, key value")
    assert(selectWhereIsNotNullResult(currentResultRow)(1) == null, s"$testnm failed on returned Row0, null col1 value")
    assert(selectWhereIsNotNullResult(currentResultRow)(2) == 12340, s"$testnm failed on returned Row0, col2 value")
    assert(selectWhereIsNotNullResult(currentResultRow)(3) == 23456780, s"$testnm failed on returned Row0, col3 value")

    currentResultRow += 1
    // check 2nd result row
    assert(selectWhereIsNotNullResult(currentResultRow)(0) === s"Row1", s"$testnm failed on returned Row1, key value")
    // skip comparison of actual and expected bytecol value
    assert(selectWhereIsNotNullResult(currentResultRow)(2) == null, s"$testnm failed on returned Row1, null col2 value")
    assert(selectWhereIsNotNullResult(currentResultRow)(3) == 23456789, s"$testnm failed on returned Row1, col3 value")


    runSql("  Drop Table insertNullValuesTest")
  }


}
