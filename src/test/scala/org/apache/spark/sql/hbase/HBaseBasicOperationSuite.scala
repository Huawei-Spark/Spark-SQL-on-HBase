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

package org.apache.spark.hsc.sql.hbase

import org.apache.spark.sql.hbase.{TestHbase, TestBaseWithSplitData}

/**
 * Test insert / query against the table
 */
class HBaseBasicOperationSuite extends TestBaseWithSplitData {

  override def beforeAll() = {
    super.beforeAll()
  }

  override def afterAll() = {
    super.afterAll()
  }

  test("Insert Into table in StringFormat") {
    hsc.sql( """CREATE TABLE tb0 (column2 INTEGER, column1 INTEGER, column4 FLOAT,
          column3 SHORT, PRIMARY KEY(column1))
          MAPPED BY (default.ht0, COLS=[column2=family0.qualifier0, column3=family1.qualifier1,
          column4=family2.qualifier2]) IN StringFormat"""
    )

    assert(hsc.sql( """SELECT * FROM tb0""").collect().length == 0)
    hsc.sql( """INSERT INTO TABLE tb0 SELECT col4,col4,col6,col3 FROM ta""")
    assert(hsc.sql( """SELECT * FROM tb0""").collect().length == 14)

    hsc.sql( """SELECT * FROM tb0""").show
    hsc.sql( """SELECT * FROM tb0 where column2 > 200""").show

    hsc.sql( """DROP TABLE tb0""")
    dropNativeHbaseTable("ht0")
  }

  test("Insert and Query Single Row") {
    hsc.sql( """CREATE TABLE tb1 (column1 INTEGER, column2 STRING,
          PRIMARY KEY(column1))
          MAPPED BY (ht1, COLS=[column2=cf.cq])"""
    )

    assert(hsc.sql( """SELECT * FROM tb1""").collect().length == 0)
    hsc.sql( """INSERT INTO TABLE tb1 VALUES (1024, "abc")""")
    hsc.sql( """INSERT INTO TABLE tb1 VALUES (1028, "abd")""")
    assert(hsc.sql( """SELECT * FROM tb1""").collect().length == 2)
    assert(
      hsc.sql( """SELECT * FROM tb1 WHERE (column1 = 1023 AND column2 ="abc")""").collect().length == 0)
    assert(hsc.sql(
      """SELECT * FROM tb1 WHERE (column1 = 1024)
        |OR (column1 = 1028 AND column2 ="abd")""".stripMargin).collect().length == 2)

    hsc.sql( """DROP TABLE tb1""")
    dropNativeHbaseTable("ht1")
  }

  test("Insert and Query Single Row in StringFormat") {
    hsc.sql( """CREATE TABLE tb1 (col1 STRING, col2 BOOL, col3 SHORT, col4 INTEGER,
           |          col5 LONG, col6 FLOAT, col7 DOUBLE,
           |          PRIMARY KEY(col1))
           |          MAPPED BY (ht2, COLS=[col2=cf1.cq11, col3=cf1.cq12, col4=cf1.cq13,
           |          col5=cf2.cq21, col6=cf2.cq22, col7=cf2.cq23]) In StringFormat""".stripMargin
    )

    assert(hsc.sql( """SELECT * FROM tb1""").collect().length == 0)
    hsc.sql( """INSERT INTO TABLE tb1 VALUES ("row1", false, 1000, 5050 , 50000 , 99.99 , 999.999)""")
    hsc.sql( """INSERT INTO TABLE tb1 VALUES ("row2", false, 99  , 10000, 9999  , 1000.1, 5000.5)""")
    hsc.sql( """INSERT INTO TABLE tb1 VALUES ("row3", true , 555 , 999  , 100000, 500.05, 10000.01)""")
    hsc.sql( """SELECT col1 FROM tb1 where col2<true order by col2""")
      .collect().zip(Seq("row1", "row2")).foreach{case (r,s) => assert(r.getString(0) == s)}
    hsc.sql( """SELECT * FROM tb1 where col3>500 order by col3""")
      .collect().zip(Seq("row3", "row1")).foreach{case (r,s) => assert(r.getString(0) == s)}
    hsc.sql( """SELECT * FROM tb1 where col4>5000 order by col4""")
      .collect().zip(Seq("row1", "row2")).foreach{case (r,s) => assert(r.getString(0) == s)}
    hsc.sql( """SELECT * FROM tb1 where col5>50000 order by col5""")
      .collect().zip(Seq("row3")).foreach{case (r,s) => assert(r.getString(0) == s)}
    hsc.sql( """SELECT * FROM tb1 where col6>500 order by col6""")
      .collect().zip(Seq("row3", "row2")).foreach{case (r,s) => assert(r.getString(0) == s)}
    hsc.sql( """SELECT * FROM tb1 where col7>5000 order by col7""")
      .collect().zip(Seq("row2", "row3")).foreach{case (r,s) => assert(r.getString(0) == s)}

    hsc.sql( """DROP TABLE tb1""")
    dropNativeHbaseTable("ht2")
  }

  test("Select test 0") {
    assert(hsc.sql( """SELECT * FROM ta""").count() == 14)
  }

  test("Count(*/1) and Non-Key Column Query") {
    assert(hsc.sql( """SELECT count(*) FROM ta""").collect()(0).get(0) == 14)
    assert(hsc.sql( """SELECT count(*) FROM ta where col2 < 8""").collect()(0).get(0) == 7)
    assert(hsc.sql( """SELECT count(*) FROM ta where col4 < 0""").collect()(0).get(0) == 7)
    assert(hsc.sql( """SELECT count(1) FROM ta where col2 < 8""").collect()(0).get(0) == 7)
    assert(hsc.sql( """SELECT count(1) FROM ta where col4 < 0""").collect()(0).get(0) == 7)
  }

  test("InSet Query") {
    assert(hsc.sql( """SELECT count(*) FROM ta where col2 IN (1, 2, 3)""").collect()(0).get(0) == 3)
    assert(hsc.sql( """SELECT count(*) FROM ta where col4 IN (1, 2, 3)""").collect()(0).get(0) == 1)
  }

  test("Point Aggregate Query") {
    hsc.sql( """CREATE TABLE tb2 (column2 INTEGER, column1 INTEGER, column4 FLOAT,
          column3 SHORT, PRIMARY KEY(column1, column2))
          MAPPED BY (default.ht0, COLS=[column3=family1.qualifier1,
          column4=family2.qualifier2])"""
    )
    hsc.sql( """INSERT INTO TABLE tb2 SELECT col4,col4,col6,col3 FROM ta""")
    val result = hsc.sql( """SELECT count(*) FROM tb2 where column1=1 AND column2=1""").collect()
    assert(result.size == 1)
    assert(result(0).get(0) == 1)
  }

  test("Select test 1 (AND, OR)") {
    assert(hsc.sql( """SELECT * FROM ta WHERE col7 = 255 OR col7 = 127""").collect().length == 2)
    assert(hsc.sql( """SELECT * FROM ta WHERE col7 < 0 AND col4 < -255""").collect().length == 4)
  }

  test("Select test 2 (WHERE)") {
    assert(hsc.sql( """SELECT * FROM ta WHERE col7 > 128""").count() == 3)
    assert(hsc.sql( """SELECT * FROM ta WHERE (col7 - 10 > 128) AND col1 = ' p255 '""").collect().length == 1)
    assert(hsc.sql( """SELECT * FROM ta WHERE (col7  > 1) AND (col7 < 1)""").collect().length == 0)
    assert(hsc.sql( """SELECT * FROM ta WHERE (col7  > 1) OR (col7 < 1)""").collect().length == 13)
    assert(hsc.sql(
      """SELECT * FROM ta WHERE
        |((col7 = 1) AND (col1 < ' p255 ') AND (col1 > ' p255 ')) OR
        |((col7 = 2) AND (col1 < ' p255 ') AND (col1 > ' p255 '))
      """.stripMargin).collect().length == 0)
    assert(hsc.sql(
      """SELECT * FROM ta WHERE
        |((col7 = 1) AND (col3 < 128) AND (col3 > 128)) OR
        |((col7 = 2) AND (col3 < 127) AND (col3 > 127))
      """.stripMargin).collect().length == 0)
  }

  test("Select test 3 (ORDER BY)") {
    val result = hsc.sql( """SELECT col1, col7 FROM ta ORDER BY col7 DESC""").collect()
    val sortedResult = result.sortWith(
      (r1, r2) => r1(1).asInstanceOf[Int] > r2(1).asInstanceOf[Int])
    for ((r1, r2) <- result zip sortedResult) {
      assert(r1.equals(r2))
    }
  }

  test("Select test 4 (join)") {
    assert(hsc.sql( """SELECT ta.col2 FROM ta join tb on ta.col4=tb.col7""").collect().length == 2)
    assert(hsc.sql( """SELECT * FROM ta FULL OUTER JOIN tb WHERE tb.col7 = 1""").collect().length == 14)
    assert(hsc.sql( """SELECT * FROM ta LEFT JOIN tb WHERE tb.col7 = 1""").collect().length == 14)
    assert(hsc.sql( """SELECT * FROM ta RIGHT JOIN tb WHERE tb.col7 = 1""").collect().length == 14)
  }

  test("Alter Add column and Alter Drop column") {
    assert(hsc.sql( """SELECT * FROM ta""").collect()(0).size == 7)
    hsc.sql( """ALTER TABLE ta ADD col8 STRING MAPPED BY (col8 = cf1.cf13)""")
    assert(hsc.sql( """SELECT * FROM ta""").collect()(0).size == 8)
    hsc.sql( """ALTER TABLE ta DROP col8""")
    assert(hsc.sql( """SELECT * FROM ta""").collect()(0).size == 7)
  }
}
