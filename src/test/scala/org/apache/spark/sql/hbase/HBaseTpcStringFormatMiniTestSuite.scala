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
 * HBase minicluster query test against stringformat encoded tbl.
 */
class HBaseTpcStringFormatMiniTestSuite extends TestBase {
  private val tableName = "store_sales_stringformat"
  private val hbaseTableName = "STORE_SALES_STRINGFORMAT"
  private val hbaseFamilies = Seq("f")

  private val csvPaths = Array("src/test/resources", "sql/hbase/src/test/resources")
  private val csvFile = "store_sales_stringformat.txt"
  private val tpath = for (csvPath <- csvPaths if new java.io.File(csvPath).exists()) yield {
    logInfo(s"Following path exists $csvPath\n")
    csvPath
  }
  private[hbase] val csvPath = tpath(0)

  override protected def beforeAll() = {
    val hbaseAdmin = TestHbase.hbaseAdmin

    /**
     * create hbase table if it does not exists
     */
    if (!hbaseAdmin.tableExists(TableName.valueOf(hbaseTableName))) {
      val descriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      hbaseFamilies.foreach { f => descriptor.addFamily(new HColumnDescriptor(f))}
      try {
        hbaseAdmin.createTable(descriptor)
      } catch {
        case e: TableExistsException =>
          logError(s"HBase table $hbaseTableName already exists.", e)
      }
    }

    /**
     * drop the existing logical table if it exists
     */
    if (TestHbase.catalog.checkLogicalTableExist(tableName)) {
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
      s"""CREATE TABLE store_sales_stringformat (
          strkey STRING,
          ss_sold_date_sk INTEGER,
          ss_sold_time_sk INTEGER,
          ss_item_sk INTEGER,
          ss_customer_sk INTEGER,
          ss_cdemo_sk INTEGER,
          ss_hdemo_sk INTEGER,
          ss_addr_sk INTEGER,
          ss_store_sk INTEGER,
          ss_promo_sk INTEGER,
          ss_ticket_number INTEGER,
          ss_quantity INTEGER,
          ss_wholesale_cost FLOAT,
          ss_list_price FLOAT,
          ss_sales_price FLOAT,
          ss_ext_discount_amt	FLOAT,
          ss_ext_sales_price FLOAT,
          ss_ext_wholesale_cost FLOAT,
          ss_ext_list_price FLOAT,
          ss_ext_tax FLOAT,
          ss_coupon_amt FLOAT,
          ss_net_paid FLOAT,
          ss_net_paid_inc_tax FLOAT,
          ss_net_profit FLOAT,
          PRIMARY KEY(strkey))
          MAPPED BY
          (STORE_SALES_STRINGFORMAT, COLS=[
            ss_sold_date_sk=f.ss_sold_date_sk,
            ss_sold_time_sk=f.ss_sold_time_sk,
            ss_item_sk=f.ss_item_sk,
            ss_customer_sk=f.ss_customer_sk,
            ss_cdemo_sk=f.ss_cdemo_sk,
            ss_hdemo_sk=f.ss_hdemo_sk,
            ss_addr_sk=f.ss_addr_sk,
            ss_store_sk=f.ss_store_sk,
            ss_promo_sk=f.ss_promo_sk,
            ss_ticket_number=f.ss_ticket_number,
            ss_quantity=f.ss_quantity,
            ss_wholesale_cost=f.ss_wholesale_cost,
            ss_list_price=f.ss_list_price,
            ss_sales_price=f.ss_sales_price,
            ss_ext_discount_amt=f.ss_ext_discount_amt,
            ss_ext_sales_price=f.ss_ext_sales_price,
            ss_ext_wholesale_cost=f.ss_ext_wholesale_cost,
            ss_ext_list_price=f.ss_ext_list_price,
            ss_ext_tax=f.ss_ext_tax,
            ss_coupon_amt=f.ss_coupon_amt,
            ss_net_paid=f.ss_net_paid,
            ss_net_paid_inc_tax=f.ss_net_paid_inc_tax,
            ss_net_profit=f.ss_net_profit
          ]) IN STRINGFORMAT""".stripMargin

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
      "' INTO TABLE " + tableName
    try {
      runSql(loadSql)
    } catch {
      case e: IllegalStateException =>
        logError(s"Error occurs while loading the data $tableName", e)
    }
  }

  override protected def afterAll() = {
    runSql("DROP TABLE " + tableName)
  }

  test("Query 0") {
    val sql = "SELECT count(1) FROM store_sales_stringformat"
    val rows = runSql(sql)
    assert(rows.size == 1)
    assert(rows(0).get(0) == 10)
  }

  test("Query 1") {
    val sql =
      s"""SELECT ss_quantity, ss_wholesale_cost, ss_list_price
         |FROM store_sales_stringformat
         |WHERE ss_item_sk = 574
         |AND ss_ticket_number = 29""".stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.size == 1)
    assert(rows(0).get(0) == 33)
    assert(rows(0).get(1) == 68.24f)
    assert(rows(0).get(2) == 116.69f)
  }

  test("Query 2") {
    val sql =
      s"""SELECT ss_sold_date_sk, ss_sold_time_sk, ss_store_sk
         |FROM store_sales_stringformat
         |WHERE ss_item_sk = 3163
         |AND ss_ticket_number = 7"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.size == 1)
    assert(rows(0).get(0) == 2452260)
    assert(rows(0).get(1) == 46712)
    assert(rows(0).get(2) == 19)
  }

  test("Query 3") {
    val sql =
      s"""SELECT ss_customer_sk, ss_promo_sk, ss_coupon_amt, ss_net_profit
         |FROM store_sales_stringformat
         |WHERE ss_item_sk = 18814
         |AND ss_ticket_number = 29"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.size == 1)
    assert(rows(0).get(0) == null)
    assert(rows(0).get(1) == null)
    assert(rows(0).get(2) == 0.00f)
    assert(rows(0).get(3) == -4398.98f)
  }

  test("Query 4") {
    val sql =
      s"""SELECT ss_ticket_number, count(1)
         |FROM store_sales_stringformat
         |GROUP BY ss_ticket_number
         |ORDER BY ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 5)

    assert(rows(0).get(0) == 7)
    assert(rows(0).get(1) == 2)

    assert(rows(1).get(0) == 10)
    assert(rows(1).get(1) == 2)

    assert(rows(2).get(0) == 11)
    assert(rows(2).get(1) == 1)

    assert(rows(3).get(0) == 29)
    assert(rows(3).get(1) == 3)

    assert(rows(4).get(0) == 30)
    assert(rows(4).get(1) == 2)
  }

  test("Query 5") {
    val sql =
      """SELECT ss_item_sk, ss_ticket_number, count(1)
        |FROM store_sales_stringformat
        |WHERE ss_item_sk > 14000 AND ss_item_sk < 18000
        |GROUP BY ss_item_sk, ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 1)
    assert(rows(0).get(0) == 16335)
    assert(rows(0).get(1) == 10)
    assert(rows(0).get(2) == 1)
  }

  test("Query 6") {
    val sql =
      s"""SELECT ss_item_sk, avg(ss_quantity) as avg_qty, count(ss_quantity) as cnt_qty
         |FROM store_sales_stringformat
         |WHERE ss_item_sk = 707
         |GROUP BY ss_item_sk
         |ORDER BY ss_item_sk"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 1)
    assert(rows(0).get(0) == 707)
    assert(rows(0).get(1) == 83.0f)
    assert(rows(0).get(2) == 1)
  }

  test("Query 7") {
    val sql =
      s"""SELECT ss_item_sk, ss_ticket_number, sum(ss_wholesale_cost) as sum_wholesale_cost
         |FROM store_sales_stringformat
         |WHERE ss_item_sk > 9000 AND ss_item_sk < 18000
         |GROUP BY ss_item_sk, ss_ticket_number
         |ORDER BY ss_item_sk, ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 2)

    assert(rows(0).get(0) == 12919)
    assert(rows(0).get(1) == 30)
    assert(rows(0).get(2) == 61.959999084472656)

    assert(rows(1).get(0) == 16335)
    assert(rows(1).get(1) == 10)
    assert(rows(1).get(2) == 82.3499984741211)
  }

  test("Query 8") {
    val sql =
      s"""SELECT ss_item_sk, ss_ticket_number,
         |min(ss_wholesale_cost) as min_wholesale_cost,
         |max(ss_wholesale_cost) as max_wholesale_cost,
         |avg(ss_wholesale_cost) as avg_wholesale_cost
         |FROM store_sales_stringformat
         |WHERE ss_item_sk > 1000 AND ss_item_sk < 18000
         |GROUP BY ss_item_sk, ss_ticket_number
         |ORDER BY ss_item_sk, ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 5)

    assert(rows(0).get(0) == 1579)
    assert(rows(0).get(1) == 30)
    assert(rows(0).get(2) == 64.0f)
    assert(rows(0).get(3) == 64.0f)
    assert(rows(0).get(4) == 64.0)

    assert(rows(2).get(0) == 3163)
    assert(rows(2).get(1) == 7)
    assert(rows(2).get(2) == 69.53f)
    assert(rows(2).get(3) == 69.53f)
    assert(rows(2).get(4) == 69.52999877929688)

    assert(rows(4).get(0) == 16335)
    assert(rows(4).get(1) == 10)
    assert(rows(4).get(2) == 82.35f)
    assert(rows(4).get(3) == 82.35f)
    assert(rows(4).get(4) == 82.3499984741211)
  }

  test("Query 9") {
    val sql =
      s"""SELECT ss_item_sk, count(ss_customer_sk) as count_ss_customer_sk
         |FROM store_sales_stringformat
         |WHERE ss_item_sk > 0 AND ss_item_sk <= 18813
         |GROUP BY ss_item_sk
         |ORDER BY ss_item_sk"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 9)

    assert(rows(0).get(0) == 7)
    assert(rows(0).get(1) == 1)

    assert(rows(1).get(0) == 574)
    assert(rows(1).get(1) == 1)

    assert(rows(2).get(0) == 707)
    assert(rows(2).get(1) == 1)

    assert(rows(3).get(0) == 1579)
    assert(rows(3).get(1) == 1)

    assert(rows(4).get(0) == 1857)
    assert(rows(4).get(1) == 1)

    assert(rows(5).get(0) == 3163)
    assert(rows(5).get(1) == 1)

    assert(rows(6).get(0) == 12919)
    assert(rows(6).get(1) == 1)

    assert(rows(7).get(0) == 16335)
    assert(rows(7).get(1) == 1)

    assert(rows(8).get(0) == 18669)
    assert(rows(8).get(1) == 1)
  }

  test("Query 10") {
    val sql = "SELECT count(*) FROM store_sales_stringformat WHERE ss_net_profit < 100"
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows(0).get(0) == 8)
  }

  test("Query 11") {
    val sql =
      s"""SELECT count(*) FROM store_sales_stringformat
         |WHERE ss_coupon_amt < 500 AND ss_ext_discount_amt < 500
         |AND ss_net_paid < 500 AND ss_net_paid_inc_tax < 500"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows(0).get(0) == 2)
  }

  test("Query 12") {
    val sql =
      s"""SELECT count(distinct ss_customer_sk) as count_distinct_customer
         |FROM store_sales_stringformat"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows(0).get(0) == 5)
  }

  test("Query 13") {
    val sql = "SELECT * FROM store_sales_stringformat LIMIT 5"
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 5)
  }

  test("Query 14") {
    val sql =
      s"""SELECT ss_customer_sk, count(*)
         |FROM store_sales_stringformat
         |WHERE ss_item_sk >= 4000 AND ss_item_sk <= 18000
         |GROUP BY ss_customer_sk
         |ORDER BY ss_customer_sk"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 2)

    assert(rows(0).get(0) == 75937)
    assert(rows(0).get(1) == 1)

    assert(rows(1).get(0) == 180451)
    assert(rows(1).get(1) == 1)
  }

  test("Query 15") {
    val sql =
      s"""SELECT count(ss_customer_sk) as count_customer
         |FROM store_sales_stringformat
         |WHERE ss_customer_sk IN (1,25,50,75937,180451)"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows(0).get(0) == 4)
  }

  test("Query 16") {
    val sql =
      s"""SELECT count(ss_customer_sk) as count_customer
         |FROM store_sales_stringformat
         |WHERE ss_customer_sk <= 147954
         |AND ss_quantity < 5000"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows(0).get(0) == 7)
  }

  test("Query 17") {
    val sql =
      s"""SELECT count(ss_customer_sk) AS count_customer
         |FROM store_sales_stringformat
         |WHERE ss_customer_sk > 100"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows(0).get(0) == 9)
  }

  test("Query 18") {
    val sql =
      s"""SELECT ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price
         |FROM store_sales_stringformat
         |WHERE ss_ticket_number = 10
         |OR ss_wholesale_cost < 17.33
         |ORDER BY ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 3)

    assert(rows(0).get(0) == 10)
    assert(rows(0).get(1) == 66)
    assert(rows(0).get(2) == 82.35f)
    assert(rows(0).get(3) == 137.52f)

    assert(rows(1).get(0) == 10)
    assert(rows(1).get(1) == 83)
    assert(rows(1).get(2) == 10.26f)
    assert(rows(1).get(3) == 17.33f)

    assert(rows(2).get(0) == 11)
    assert(rows(2).get(1) == 68)
    assert(rows(2).get(2) == 7.16f)
    assert(rows(2).get(3) == 12.88f)
  }

  test("Query 19") {
    val sql =
      s"""SELECT ss_ticket_number, ss_sold_date_sk, ss_sold_time_sk, ss_store_sk
         |FROM store_sales_stringformat
         |WHERE ss_ticket_number = 10 OR ss_sold_date_sk >= 2451966
         |ORDER BY ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 5)

    assert(rows(0).get(0) == 7)
    assert(rows(0).get(1) == 2452260)
    assert(rows(0).get(2) == 46712)
    assert(rows(0).get(3) == 19)

    assert(rows(1).get(0) == 7)
    assert(rows(1).get(1) == 2452260)
    assert(rows(1).get(2) == 46712)
    assert(rows(1).get(3) == 19)

    assert(rows(2).get(0) == 10)
    assert(rows(2).get(1) == 2451966)
    assert(rows(2).get(2) == 60226)
    assert(rows(2).get(3) == 13)

    assert(rows(3).get(0) == 10)
    assert(rows(3).get(1) == 2451966)
    assert(rows(3).get(2) == 60226)
    assert(rows(3).get(3) == 13)

    assert(rows(4).get(0) == 11)
    assert(rows(4).get(1) == 2452420)
    assert(rows(4).get(2) == 68961)
    assert(rows(4).get(3) == 25)
  }

  test("Query 20") {
    val sql =
      s"""SELECT ss_ticket_number, ss_sold_date_sk, ss_customer_sk, ss_promo_sk, ss_coupon_amt
         |FROM store_sales_stringformat
         |WHERE ss_ticket_number = 10
         |OR (ss_sold_date_sk > 2451121 AND ss_sold_date_sk <= 2451966)
         |ORDER BY ss_ticket_number""".stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 4)

    assert(rows(0).get(0) == 10)
    assert(rows(0).get(1) == 2451966)
    assert(rows(0).get(2) == 180451)
    assert(rows(0).get(3) == 145)
    assert(rows(0).get(4) == 0.00f)

    assert(rows(1).get(0) == 10)
    assert(rows(1).get(1) == 2451966)
    assert(rows(1).get(2) == 180451)
    assert(rows(1).get(3) == 175)
    assert(rows(1).get(4) == 0.00f)

    assert(rows(2).get(0) == 30)
    assert(rows(2).get(1) == 2451390)
    assert(rows(2).get(2) == 75937)
    assert(rows(2).get(3) == 231)
    assert(rows(2).get(4) == 0.00f)

    assert(rows(3).get(0) == 30)
    assert(rows(3).get(1) == 2451390)
    assert(rows(3).get(2) == 75937)
    assert(rows(3).get(3) == 200)
    assert(rows(3).get(4) == 210.72f)
  }

  test("Query 21") {
    val sql =
      s"""SELECT strkey, ss_item_sk, ss_ticket_number, count(1)
         |FROM store_sales_stringformat
         |WHERE ss_ticket_number >= 10 and ss_ticket_number <= 20
         |GROUP BY strkey, ss_item_sk, ss_ticket_number
         |ORDER BY strkey, ss_item_sk, ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 3)

    assert(rows(0).get(0) == "00707000000010")
    assert(rows(0).get(1) == 707)
    assert(rows(0).get(2) == 10)
    assert(rows(0).get(3) == 1)

    assert(rows(1).get(0) == "16335000000010")
    assert(rows(1).get(1) == 16335)
    assert(rows(1).get(2) == 10)
    assert(rows(1).get(3) == 1)

    assert(rows(2).get(0) == "18669000000011")
    assert(rows(2).get(1) == 18669)
    assert(rows(2).get(2) == 11)
    assert(rows(2).get(3) == 1)
  }

  test("Query 22") {
    val sql =
      s"""SELECT strkey, ss_item_sk, ss_ticket_number, SUM(ss_wholesale_cost) AS sum_wholesale_cost
         |FROM store_sales_stringformat
         |WHERE ss_ticket_number >= 10 and ss_ticket_number <= 20
         |GROUP BY strkey, ss_item_sk, ss_ticket_number
         |ORDER BY strkey, ss_item_sk, ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)

    assert(rows.length == 3)

    assert(rows(0).get(0) == "00707000000010")
    assert(rows(0).get(1) == 707)
    assert(rows(0).get(2) == 10)
    assert(rows(0).get(3) == 10.260000228881836)

    assert(rows(1).get(0) == "16335000000010")
    assert(rows(1).get(1) == 16335)
    assert(rows(1).get(2) == 10)
    assert(rows(1).get(3) == 82.3499984741211)

    assert(rows(2).get(0) == "18669000000011")
    assert(rows(2).get(1) == 18669)
    assert(rows(2).get(2) == 11)
    assert(rows(2).get(3) == 7.159999847412109)
  }



  test("Query 23") {
    val sql =
      s"""SELECT ss_item_sk, ss_ticket_number,
         |min(ss_wholesale_cost) as min_wholesale_cost,
         |max(ss_wholesale_cost) as max_wholesale_cost,
         |avg(ss_wholesale_cost) as avg_wholesale_cost
         |FROM store_sales_stringformat
         |WHERE (ss_ticket_number >= 10 AND ss_ticket_number <= 20)
         |AND (ss_sold_date_sk > 2451121 AND ss_sold_date_sk <= 2451966)
         |GROUP BY ss_item_sk, ss_ticket_number
         |ORDER BY ss_item_sk, ss_ticket_number""".stripMargin
    val rows = runSql(sql)
    // printRows(rows)

    assert(rows.length == 2)

    assert(rows(0).get(0) == 707)
    assert(rows(0).get(1) == 10)
    assert(rows(0).get(2) == 10.26f)
    assert(rows(0).get(3) == 10.26f)
    assert(rows(0).get(4) == 10.260000228881836)

    assert(rows(1).get(0) == 16335)
    assert(rows(1).get(1) == 10)
    assert(rows(1).get(2) == 82.35f)
    assert(rows(1).get(3) == 82.35f)
    assert(rows(1).get(4) == 82.3499984741211)
  }

  test("Query 24") {
    val sql =
      s"""SELECT ss_item_sk, ss_ticket_number,
         |min(ss_ext_wholesale_cost) as min_ss_ext_wholesale_cost,
         |max(ss_ext_wholesale_cost) as max_ss_ext_wholesale_cost,
         |avg(ss_ext_wholesale_cost) as avg_ss_ext_wholesale_cost
         |FROM store_sales_stringformat
         |WHERE (ss_ticket_number >= 10 AND ss_ticket_number <= 100)
         |AND (ss_customer_sk > 0 AND ss_customer_sk <= 147954)
         |AND (ss_sold_date_sk = 2451121 OR ss_sold_date_sk = 2451390)
         |GROUP BY ss_item_sk, ss_ticket_number
         |ORDER BY ss_item_sk, ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)

    assert(rows.length == 4)

    assert(rows(0).get(0) == 7)
    assert(rows(0).get(1) == 29)
    assert(rows(0).get(2) == 1726.89f)
    assert(rows(0).get(3) == 1726.89f)
    assert(rows(0).get(4) == 1726.8900146484375)

    assert(rows(1).get(0) == 574)
    assert(rows(1).get(1) == 29)
    assert(rows(1).get(2) == 2251.92f)
    assert(rows(1).get(3) == 2251.92f)
    assert(rows(1).get(4) == 2251.919921875)

    assert(rows(2).get(0) == 1579)
    assert(rows(2).get(1) == 30)
    assert(rows(2).get(2) == 1344.0f)
    assert(rows(2).get(3) == 1344.0f)
    assert(rows(2).get(4) == 1344.0)

    assert(rows(3).get(0) == 12919)
    assert(rows(3).get(1) == 30)
    assert(rows(3).get(2) == 2044.68f)
    assert(rows(3).get(3) == 2044.68f)
    assert(rows(3).get(4) == 2044.6800537109375)
  }

  test("Query 25") {
    val sql =
      s"""SELECT *
         |FROM store_sales_stringformat
         |WHERE strkey > '03163000000007'"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)

    assert(rows.length == 4)

    assert(rows(0).get(0) == "12919000000030")
    assert(rows(0).get(1) == 2451390)
    assert(rows(0).get(5) == 499127)
    assert(rows(0).get(23) == -1765.35f)

    assert(rows(1).get(0) == "16335000000010")
    assert(rows(1).get(1) == 2451966)
    assert(rows(1).get(5) == 71288)
    assert(rows(1).get(23) == 10.56f)

    assert(rows(2).get(0) == "18669000000011")
    assert(rows(2).get(1) == 2452420)
    assert(rows(2).get(5) == 781292)
    assert(rows(2).get(23) == -209.76f)

    assert(rows(3).get(0) == "18814000000029")
    assert(rows(3).get(1) == 2451121)
    assert(rows(3).get(5) == null)
    assert(rows(3).get(23) == -4398.98f)
  }

  test("Query 26") {
    val sql =
      s"""SELECT *
         |FROM store_sales_stringformat
         |WHERE ss_wholesale_cost >= 33
         |AND ss_quantity > 40"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)

    assert(rows.length == 3)

    assert(rows(0).get(0) == "01857000000007")
    assert(rows(0).get(1) == 2452260)
    assert(rows(0).get(5) == 890396)
    assert(rows(0).get(23) == 1150.23f)

    assert(rows(1).get(0) == "03163000000007")
    assert(rows(1).get(1) == 2452260)
    assert(rows(1).get(5) == 890396)
    assert(rows(1).get(23) == -2900.34f)

    assert(rows(2).get(0) == "16335000000010")
    assert(rows(2).get(1) == 2451966)
    assert(rows(2).get(5) == 71288)
    assert(rows(2).get(23) == 10.56f)
  }

  test("Query 27") {
    val sql =
      s"""SELECT * FROM store_sales_stringformat
         |WHERE ss_ticket_number + 0 = 10
         |AND ss_sold_date_sk + 0 > 0"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)

    assert(rows.length == 2)

    assert(rows(0).get(0) == "00707000000010")
    assert(rows(0).get(2) == 60226)
    assert(rows(0).get(8) == 13)
    assert(rows(0).get(23) == -89.64f)

    assert(rows(1).get(0) == "16335000000010")
    assert(rows(1).get(2) == 60226)
    assert(rows(1).get(8) == 13)
    assert(rows(1).get(23) == 10.56f)
  }

  test("Query 28") {
    val sql =
      s"""SELECT * FROM store_sales_stringformat
         |WHERE ss_cdemo_sk IS NULL"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)

    assert(rows.length == 1)

    assert(rows(0).get(0) == "18814000000029")
    assert(rows(0).get(2) == null)
    assert(rows(0).get(8) == null)
    assert(rows(0).get(23) == -4398.98f)
  }

  test("Query 29") {
    val sql =
      s"""SELECT * FROM store_sales_stringformat
         |WHERE ss_cdemo_sk IS NOT NULL"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 9)
  }

  test("Query 30") {
    val sql =
      s"""SELECT * FROM store_sales_stringformat
         |WHERE ss_cdemo_sk IS NOT NULL
         |AND ss_ticket_number = 29"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 2)

    assert(rows(0).get(0) == "00007000000029")
    assert(rows(0).get(2) == 45001)
    assert(rows(0).get(8) == 14)
    assert(rows(0).get(23) == 1192.95f)

    assert(rows(1).get(0) == "00574000000029")
    assert(rows(1).get(2) == 45001)
    assert(rows(1).get(8) == 14)
    assert(rows(1).get(23) == -1421.81f)
  }

  test("Query 31") {
    val sql =
      s"""SELECT * FROM store_sales_stringformat
         |WHERE ss_cdemo_sk IS NULL
         |AND ss_ticket_number = 29"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)
    assert(rows.length == 1)

    assert(rows(0).get(0) == "18814000000029")
    assert(rows(0).get(2) == null)
    assert(rows(0).get(8) == null)
    assert(rows(0).get(22) == null)
    assert(rows(0).get(23) == -4398.98f)
  }

  test("Query 32") {
    val sql =
      s"""SELECT * FROM store_sales_stringformat
         |WHERE ss_cdemo_sk IS NULL
         |OR ss_ticket_number = 29"""
        .stripMargin
    val rows = runSql(sql)
    // printRows(rows)

    assert(rows.length == 3)

    assert(rows(0).get(0) == "00007000000029")
    assert(rows(0).get(2) == 45001)
    assert(rows(0).get(8) == 14)
    assert(rows(0).get(22) == 2949.03f)
    assert(rows(0).get(23) == 1192.95f)

    assert(rows(1).get(0) == "00574000000029")
    assert(rows(1).get(2) == 45001)
    assert(rows(1).get(8) == 14)
    assert(rows(1).get(22) == 896.51f)
    assert(rows(1).get(23) == -1421.81f)

    assert(rows(2).get(0) == "18814000000029")
    assert(rows(2).get(2) == null)
    assert(rows(2).get(8) == null)
    assert(rows(2).get(22) == null)
    assert(rows(2).get(23) == -4398.98f)
  }
}
