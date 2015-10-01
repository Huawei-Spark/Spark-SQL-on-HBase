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
 * This is a mini tpc test suite running against mini-cluster
 */
class HBaseTpcMiniTestSuite extends TestBase {
  private val tableName = "store_sales"
  private val hbaseTableName = "store_sales_htable"
  private val hbaseFamilies = Seq("f")

  private val csvPaths = Array("src/test/resources", "sql/hbase/src/test/resources")
  private val csvFile = "store_sales.txt"
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
      val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
      hbaseFamilies.foreach { f => descriptor.addFamily(new HColumnDescriptor(f))}
      try {
        hbaseAdmin.createTable(descriptor)
      } catch {
        case e: TableExistsException =>
          logError(s"Table already exists $tableName", e)
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
      s"""CREATE TABLE store_sales(
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
        PRIMARY KEY(ss_item_sk, ss_ticket_number))
        MAPPED BY
        (store_sales_htable, COLS=[
          ss_sold_date_sk=f.ss_sold_date_sk,
          ss_sold_time_sk=f.ss_sold_time_sk,
          ss_customer_sk=f.ss_customer_sk,
          ss_cdemo_sk=f.ss_cdemo_sk,
          ss_hdemo_sk=f.ss_hdemo_sk,
          ss_addr_sk=f.ss_addr_sk,
          ss_store_sk=f.ss_store_sk,
          ss_promo_sk=f.ss_promo_sk,
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
      "' INTO TABLE store_sales"
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
    val sql = "SELECT count(1) FROM store_sales"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 100)
  }

  test("Query 1") {
    val sql = "SELECT ss_quantity, ss_wholesale_cost, ss_list_price FROM store_sales WHERE ss_item_sk = 2744 AND ss_ticket_number = 1"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 37)
    assert(rows(0).get(1) == 63.63f)
    assert(rows(0).get(2) == 101.17f)
  }

  test("Query 2") {
    val sql = "SELECT ss_sold_date_sk, ss_sold_time_sk, ss_store_sk FROM store_sales WHERE ss_item_sk = 2744 AND ss_ticket_number = 1"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 2451813)
    assert(rows(0).get(1) == 65495)
    assert(rows(0).get(2) == 25)
  }

  test("Query 3") {
    val sql = "SELECT ss_customer_sk, ss_promo_sk, ss_coupon_amt FROM store_sales WHERE ss_item_sk = 2744 AND ss_ticket_number = 1"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 225006)
    assert(rows(0).get(1) == 354)
    assert(rows(0).get(2) == 46.03f)
  }

  test("Query 4") {
    val sql = "SELECT ss_item_sk, count(1) FROM store_sales GROUP BY ss_item_sk"
    val rows = runSql(sql)
    assert(rows.length == 100)
  }

  test("Query 5") {
    val sql = "SELECT ss_item_sk, ss_ticket_number, count(1) FROM store_sales WHERE ss_item_sk > 4000 AND ss_item_sk < 5000 GROUP BY ss_item_sk, ss_ticket_number"
    val rows = runSql(sql)
    assert(rows.length == 5)
  }

  test("Query 6") {
    val sql = "SELECT ss_item_sk, avg(ss_quantity) as avg_qty, count(ss_quantity) as cnt_qty FROM store_sales WHERE ss_item_sk = 2744 GROUP BY ss_item_sk"
    val rows = runSql(sql)
    assert(rows.length == 1)
  }

  test("Query 7") {
    val sql =
      s"""SELECT ss_item_sk, ss_ticket_number, sum(ss_wholesale_cost) as sum_wholesale_cost
         |FROM store_sales
         |WHERE ss_item_sk > 4000 AND ss_item_sk <= 5000
         |GROUP BY ss_item_sk, ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    assert(rows.length == 5)
  }

  test("Query 7.1") {
    val sql =
      s"""SELECT ss_item_sk, ss_ticket_number, sum(ss_wholesale_cost) as sum_wholesale_cost
         |FROM store_sales
         |WHERE ss_item_sk > 17182
         |AND ss_item_sk <= 17183
         |GROUP BY ss_item_sk, ss_ticket_number"""
        .stripMargin
    val rows = runSql(sql)
    assert(rows.length == 1)
    assert(rows(0)(0) == 17183)
    assert(rows(0)(1) == 6)
    assert(rows(0)(2) == null) // null-input -> sum() -> null-output
  }

  test("Query 8") {
    val sql = "SELECT ss_item_sk, ss_ticket_number, min(ss_wholesale_cost) as min_wholesale_cost, max(ss_wholesale_cost) as max_wholesale_cost, avg(ss_wholesale_cost) as avg_wholesale_cost FROM store_sales WHERE ss_item_sk > 4000 AND ss_item_sk <= 5000 GROUP BY ss_item_sk, ss_ticket_number"
    val rows = runSql(sql)
    assert(rows.length == 5)
  }

  test("Query 9") {
    val sql = "SELECT ss_item_sk, count(ss_customer_sk) as count_ss_customer_sk FROM store_sales WHERE ss_item_sk > 4000 AND ss_item_sk <= 5000 GROUP BY ss_item_sk"
    val rows = runSql(sql)
    assert(rows.length == 5)
  }

  test("Query 10") {
    val sql = "SELECT count(*) FROM store_sales WHERE ss_net_profit < 100"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 74)
  }

  test("Query 11") {
    val sql = "SELECT count(*) FROM store_sales WHERE ss_coupon_amt < 50 AND ss_ext_discount_amt < 50 AND ss_net_paid < 50 AND ss_net_paid_inc_tax < 50"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 6)
  }

  test("Query 12") {
    val sql = "SELECT count(distinct ss_customer_sk) as count_distinct_customer FROM store_sales"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 8)
  }

  test("Query 13") {
    val sql = "SELECT * FROM store_sales limit 100"
    val rows = runSql(sql)
    assert(rows.length == 100)
  }

  test("Query 14") {
    val sql = "SELECT ss_customer_sk, count(*) FROM store_sales WHERE ss_item_sk >= 4000 AND ss_item_sk <= 5000 GROUP BY ss_customer_sk"
    val rows = runSql(sql)
    assert(rows.length == 5)
  }

  test("Query 15") {
    val sql = "SELECT ss_customer_sk FROM store_sales WHERE ss_customer_sk IN (1,25,50,75,100)"
    val rows = runSql(sql)
    assert(rows.size == 0)
  }

  test("Query 15.1") {
    val sql = "SELECT ss_customer_sk FROM store_sales WHERE ss_customer_sk IN (1,194284)"
    val rows = runSql(sql)
    assert(rows.size == 14)
  }

  test("Query 15.2") {
    val sql = "SELECT ss_customer_sk FROM store_sales WHERE ss_customer_sk IN (1,225006)"
    val rows = runSql(sql)
    assert(rows.size == 14)
  }

  test("Query 15.3") {
    val sql = "SELECT ss_customer_sk FROM store_sales WHERE ss_customer_sk IN (1,194284,225006)"
    val rows = runSql(sql)
    assert(rows.size == 28)
  }

  test("Query 16") {
    val sql = "SELECT count(ss_customer_sk) as count_customer FROM store_sales WHERE ss_customer_sk < 100 AND ss_quantity < 5"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 2)
  }

  test("Query 17") {
    val sql = "SELECT count(ss_customer_sk) AS count_customer FROM store_sales WHERE ss_customer_sk > 100"
    val rows = runSql(sql)
    assert(rows(0).get(0) == 83)
  }

  test("Query 18") {
    val sql = "SELECT ss_quantity, ss_wholesale_cost, ss_list_price FROM store_sales WHERE ss_ticket_number = 3"
    val rows = runSql(sql)
    assert(rows.length == 14)
  }

  test("Query 19") {
    val sql = "SELECT ss_sold_date_sk, ss_sold_time_sk, ss_store_sk FROM store_sales WHERE ss_ticket_number = 3"
    val rows = runSql(sql)
    assert(rows.length == 14)
  }

  test("Query 20") {
    val sql = "SELECT ss_customer_sk, ss_promo_sk, ss_coupon_amt FROM store_sales WHERE ss_ticket_number = 3"
    val rows = runSql(sql)
    assert(rows.length == 14)
  }

  test("Query 21") {
    val sql = "SELECT ss_item_sk, ss_ticket_number, count(1) FROM store_sales WHERE ss_ticket_number >= 3 and ss_ticket_number <= 4 group by ss_item_sk, ss_ticket_number"
    val rows = runSql(sql)
    assert(rows.length == 24)
  }

  test("Query 22") {
    val sql = "SELECT ss_item_sk, ss_ticket_number, SUM(ss_wholesale_cost) AS sum_wholesale_cost FROM store_sales WHERE ss_ticket_number >= 3 AND ss_ticket_number <= 4 group by ss_item_sk, ss_ticket_number"
    val rows = runSql(sql)
    assert(rows.length == 24)
  }

  test("Query 23") {
    val sql = "SELECT ss_item_sk, ss_ticket_number, min(ss_wholesale_cost) as min_wholesale_cost, max(ss_wholesale_cost) as max_wholesale_cost, avg(ss_wholesale_cost) as avg_wholesale_cost FROM store_sales WHERE ss_ticket_number >= 3 and ss_ticket_number <= 3 GROUP BY ss_item_sk, ss_ticket_number"
    val rows = runSql(sql)
    assert(rows.length == 14)
  }

  test("Query 24") {
    val sql = "SELECT ss_item_sk, ss_ticket_number FROM store_sales WHERE (ss_item_sk = 186 AND ss_ticket_number > 0)"
    val rows = runSql(sql)
    assert(rows.length == 1)
  }

  test("Query 25") {
    val sql = "SELECT * FROM store_sales WHERE ss_ticket_number > 6 and ss_sold_date_sk > 0"
    val rows = runSql(sql)
    assert(rows.length == 21)
  }

  test("Query 26") {
    val sql = "SELECT * FROM store_sales WHERE ss_ticket_number = 7 and ss_sold_date_sk > 0"
    val rows = runSql(sql)
    assert(rows.length == 12)
  }

  test("Query 27") {
    val sql = "SELECT * FROM store_sales WHERE ss_ticket_number + 0 = 3 and ss_sold_date_sk + 0 > 0"
    val rows = runSql(sql)
    assert(rows.length == 13)
  }

  test("Query 28") {
    val sql = "SELECT * FROM store_sales WHERE ss_sold_date_sk IS NULL"
    val rows = runSql(sql)
    assert(rows.length == 5)
  }

  test("Query 29") {
    val sql = "SELECT * FROM store_sales WHERE ss_sold_date_sk IS NOT NULL"
    val rows = runSql(sql)
    assert(rows.length == 95)
  }

  test("Query 30") {
    val sql = "SELECT * FROM store_sales WHERE ss_sold_date_sk IS NOT NULL AND ss_ticket_number = 3"
    val rows = runSql(sql)
    assert(rows.length == 13)
  }

  test("Query 31") {
    val sql = "SELECT * FROM store_sales WHERE ss_sold_date_sk IS NULL AND ss_ticket_number = 3"
    val rows = runSql(sql)
    assert(rows.length == 1)
  }

  test("Query 32") {
    val sql = "SELECT * FROM store_sales WHERE ss_sold_date_sk IS NULL OR ss_ticket_number = 3"
    val rows = runSql(sql)
    assert(rows.length == 18)
  }
}
