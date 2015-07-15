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

class HBaseAggregateQueriesSuite extends
TestBaseWithNonSplitData {
  var testnm = "Group by with cols in select list and with order by"
  test("Group by with cols in select list and with order by") {
    val query =
      s"""SELECT count(1) as cnt, intcol, floatcol, strcol, max(bytecol) bytecol, max(shortcol) shortcol,
          max(floatcol) floatcolmax, max(doublecol) doublecol, max(longcol) from $TestTableName
          WHERE strcol LIKE '%Row%' AND shortcol < 12345 AND doublecol > 5678912.345681
          AND doublecol < 5678912.345684
          GROUP BY intcol, floatcol, strcol ORDER BY strcol DESC"""

    testGroupBy(testnm, query)
  }

  testnm = "Group by with cols in select list and with having and order by"
  test("Group by with cols in select list and with having and order by") {
    val query = s"""SELECT count(1) as cnt, intcol, floatcol, strcol, max(bytecol) bytecolmax,
         max(shortcol) shortcolmax, max(floatcol) floatcolmax, max(doublecol) doublecolmax,
         max(longcol) longcolmax
         FROM $TestTableName
         WHERE strcol like '%Row%' AND shortcol < 12345 AND doublecol > 5678912.345681
         AND doublecol < 5678912.345685
         GROUP BY intcol, floatcol, strcol
         HAVING max(doublecol) < 5678912.345684
         ORDER BY strcol DESC""".stripMargin
    testGroupBy(testnm, query)
  }

  def testGroupBy(testName: String, query: String) = {
    val result1 = runSql(query)
    assert(result1.length == 2, s"$testName failed on size")
    val exparr = Array(
      Array(1, 23456783, 45657.83F, "Row3", 'c', 12343, 45657.83F, 5678912.345683, 3456789012343L),
      Array(1, 23456782, 45657.82F, "Row2", 'b', 12342, 45657.82F, 5678912.345682, 3456789012342L))

    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    logInfo(s"$query came back with $result1.length results")
    logInfo(result1.mkString)

    logInfo(s"Test $testName completed successfully")
  }

  testnm = "Another Group by with cols in select list and with having and order by"
  test("Another Group by with cols in select list and with having and order by") {
    val query1 =
      s"""SELECT count(1) as cnt, intcol, floatcol, strcol, max(bytecol) bytecolmax, max(shortcol) shortcolmax,
          max(floatcol) floatcolmax, max(doublecol) doublecolmax, max(longcol) longcolmax FROM $TestTableName
          WHERE strcol LIKE '%Row%' AND shortcol < 12345 AND doublecol > 5678912.345681
          AND doublecol < 5678912.345685
          GROUP BY intcol, floatcol, strcol HAVING max(doublecol) < 5678912.345684 ORDER BY strcol DESC"""
        .stripMargin

    val result1 = runSql(query1)
    assert(result1.length == 2, s"$testnm failed on size")
    val exparr = Array(
      Array(1, 23456783, 45657.83F, "Row3", 'c', 12343, 45657.83F, 5678912.345683, 3456789012343L),
      Array(1, 23456782, 45657.82F, "Row2", 'b', 12342, 45657.82F, 5678912.345682, 3456789012342L))

    val res = {
      for (rx <- exparr.indices)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    logInfo(s"$query1 came back with $result1.length results")
    logInfo(result1.mkString)

    logInfo(s"Test $testnm completed successfully")
  }
}

