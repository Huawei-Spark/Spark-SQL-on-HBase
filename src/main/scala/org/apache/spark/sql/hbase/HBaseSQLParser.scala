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

import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.catalyst.{AbstractSparkSQLParser, SqlParser}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hbase.execution._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.Utils

//object HBaseSQLParser {
//  def getKeywords: Seq[String] = {
//    val hbaseSqlFields =
//      Class.forName("org.apache.spark.sql.hbase.HBaseSQLParser").getDeclaredFields
//    val sparkSqlFields = Class.forName("org.apache.spark.sql.catalyst.SqlParser").getDeclaredFields
//    var keywords = hbaseSqlFields.filter(x => x.getName.charAt(0).isUpper).map(_.getName)
//    keywords ++= sparkSqlFields.filter(x => x.getName.charAt(0).isUpper).map(_.getName)
//    keywords.toSeq
//  }
//}

object HBaseSQLParser extends AbstractSparkSQLParser with DataTypeParser {

  protected val ADD = Keyword("ADD")
  protected val ALTER = Keyword("ALTER")
  protected val COLS = Keyword("COLS")
  protected val CREATE = Keyword("CREATE")
  protected val DATA = Keyword("DATA")
  protected val DESCRIBE = Keyword("DESCRIBE")
  protected val DROP = Keyword("DROP")
  protected val EXISTS = Keyword("EXISTS")
  protected val FIELDS = Keyword("FIELDS")
  protected val INPATH = Keyword("INPATH")
  protected val KEY = Keyword("KEY")
  protected val LOAD = Keyword("LOAD")
  protected val LOCAL = Keyword("LOCAL")
  protected val MAPPED = Keyword("MAPPED")
  protected val PRIMARY = Keyword("PRIMARY")
  protected val PARALL = Keyword("PARALL")
  protected val SHOW = Keyword("SHOW")
  protected val TABLES = Keyword("TABLES")
  protected val VALUES = Keyword("VALUES")
  protected val TERMINATED = Keyword("TERMINATED")

  // Copy from Spark code, due to unable to leverage SqlParser
  protected val INSERT = Keyword("INSERT")
  protected val INTO = Keyword("INTO")
  protected val TABLE = Keyword("TABLE")
  protected val IN = Keyword("IN")
  protected val BY = Keyword("BY")
  protected val INTERVAL = Keyword("INTERVAL")
  protected val NULL = Keyword("NULL")
  protected val TRUE = Keyword("TRUE")
  protected val FALSE = Keyword("FALSE")

  override protected lazy val start: Parser[LogicalPlan] =
      create | drop | alterDrop | alterAdd |
      insertValues | load | show | describe

  protected lazy val insertValues: Parser[LogicalPlan] =
    INSERT ~> INTO ~> TABLE ~> ident ~ (VALUES ~> "(" ~> values <~ ")") ^^ {
      case tableName ~ valueSeq =>
        val valueStringSeq = valueSeq.map { case v =>
          if (v.value == null) null
          else v.value.toString
        }
        InsertValueIntoTableCommand(tableName, valueStringSeq)
    }

  protected lazy val create: Parser[LogicalPlan] =
    CREATE ~> TABLE ~> ident ~
      ("(" ~> tableCols <~ ",") ~
      (PRIMARY ~> KEY ~> "(" ~> keys <~ ")" <~ ")") ~
      (MAPPED ~> BY ~> "(" ~> opt(nameSpace)) ~
      (ident <~ ",") ~
      (COLS ~> "=" ~> "[" ~> mappings <~ "]" <~ ")") ~
      (IN ~> ident).? <~ opt(";") ^^ {

      case tableName ~ tableColumns ~ keySeq ~
        tableNameSpace ~ hbaseTableName ~ mappingInfo ~ encodingFormat =>
        // Since the lexical can not recognize the symbol "=" as we expected, we compose it
        // to expression first and then translate it into Map[String, (String, String)].
        // TODO: Now get the info by hacking, need to change it into normal way if possible
        val infoMap =
          mappingInfo.map (e => {
            val info = e._2.substring(1).split('.')
            if (info.length != 2) throw new Exception("\nSyntax Error of Create Table")
           (e._1.substring(1),(info(0), info(1)))
          }).toMap

        // Check whether the column info are correct or not
        val tableColSet = tableColumns.unzip._1.toSet
        val keySet = keySeq.toSet
        if (tableColSet.size != tableColumns.length ||
          keySet.size != keySeq.length ||
          !(keySet union infoMap.keySet).equals(tableColSet) ||
          (keySet intersect infoMap.keySet).nonEmpty
        ) {
          throw new Exception(
            "The Column Info of Create Table are not correct")
        }

        val customizedNameSpace = tableNameSpace.getOrElse("")

        val divideTableColsByKeyOrNonkey = tableColumns.partition {
          case (name, _) =>
            keySeq.contains(name)
        }
        val dataTypeOfKeyCols = divideTableColsByKeyOrNonkey._1
        val dataTypeOfNonkeyCols = divideTableColsByKeyOrNonkey._2

        // Get Key Info
        val keyColsWithDataType = keySeq.map {
          key => {
            val typeOfKey = dataTypeOfKeyCols.find(_._1 == key).get._2
            (key, typeOfKey)
          }
        }

        // Get Nonkey Info
        val nonKeyCols = dataTypeOfNonkeyCols.map {
          case (name, typeOfData) =>
            val infoElem = infoMap.get(name).get
            (name, typeOfData, infoElem._1, infoElem._2)
        }

        val colsSeqString = tableColumns.unzip._1.reduceLeft(_ + "," + _)
        val keyColsString = keyColsWithDataType
          .map(k => k._1 + "," + k._2)
          .reduceLeft(_ + ";" + _)
        val nonkeyColsString = if (nonKeyCols.isEmpty) ""
        else {
          nonKeyCols
            .map(k => k._1 + "," + k._2 + "," + k._3 + "," + k._4)
            .reduceLeft(_ + ";" + _)
        }

        val opts: Map[String, String] = Seq(
          ("tableName", tableName),
          ("namespace", customizedNameSpace),
          ("hbaseTableName", hbaseTableName),
          ("colsSeq", colsSeqString),
          ("keyCols", keyColsString),
          ("nonKeyCols", nonkeyColsString),
          ("encodingFormat", encodingFormat.getOrElse("binaryformat").toLowerCase)
        ).toMap

        CreateTable(tableName, "org.apache.spark.sql.hbase.HBaseSource", opts)
    }

  private[hbase] case class CreateTable(
                                         tableName: String,
                                         provider: String,
                                         options: Map[String, String]) extends RunnableCommand {
    // create table of persistent metadata
    def run(sqlContext: SQLContext) = {
      val loader = Utils.getContextOrSparkClassLoader
      val clazz: Class[_] = try loader.loadClass(provider) catch {
        case cnf: java.lang.ClassNotFoundException =>
          try loader.loadClass(provider + ".DefaultSource") catch {
            case cnf: java.lang.ClassNotFoundException =>
              sys.error(s"Failed to load class for data source: $provider")
          }
      }
      val dataSource = clazz.newInstance()
        .asInstanceOf[org.apache.spark.sql.sources.RelationProvider]
      dataSource.createRelation(sqlContext, options)
      Seq.empty
    }
  }

  protected lazy val drop: Parser[LogicalPlan] =
    DROP ~> TABLE ~> ident <~ opt(";") ^^ {
      case tableName => DropHbaseTableCommand(tableName)
    }

  protected lazy val alterDrop: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> ident ~
      (DROP ~> ident) <~ opt(";") ^^ {
      case tableName ~ colName => AlterDropColCommand(tableName, colName)
    }

  protected lazy val alterAdd: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> ident ~
      (ADD ~> tableCol) ~
      (MAPPED ~> BY ~> "(" ~> mappings<~ ")") ^^ {
      case tableName ~ tableColumn ~ mappingInfo =>
        // Since the lexical can not recognize the symbol "=" as we expected, we compose it
        // to expression first and then translate it into Map[String, (String, String)]
        // TODO: Now get the info by hacking, need to change it into normal way if possible
        val infoMap =
          mappingInfo.map (e => {
            val info = e._2.substring(1).split('.')
            if (info.length != 2) throw new Exception("\nSyntax Error of Create Table")
            (e._1.substring(1),(info(0), info(1)))
          }).toMap
        val familyAndQualifier = infoMap(tableColumn._1)

        AlterAddColCommand(tableName, tableColumn._1, tableColumn._2,
          familyAndQualifier._1, familyAndQualifier._2)
    }

  // Load syntax:
  // LOAD DATA [LOCAL] INPATH filePath INTO TABLE tableName [FIELDS TERMINATED BY char]
  protected lazy val load: Parser[LogicalPlan] =
    (LOAD ~> PARALL.?) ~ (DATA ~> LOCAL.?) ~
      (INPATH ~> stringLit) ~
      (INTO ~> TABLE ~> ident) ~
      (FIELDS ~> TERMINATED ~> BY ~> stringLit).? <~ opt(";") ^^ {
      case isparall ~ isLocal ~ filePath ~ table ~ delimiter =>
        BulkLoadIntoTableCommand(
          filePath, table, isLocal.isDefined,
          delimiter, isparall.isDefined)
    }

  // syntax:
  // SHOW TABLES
  protected lazy val show: Parser[LogicalPlan] =
    SHOW ~> TABLES <~ opt(";") ^^^ ShowTablesCommand

  protected lazy val describe: Parser[LogicalPlan] =
    (DESCRIBE ~> ident) ^^ {
      case tableName => DescribeTableCommand(tableName)
    }

  override protected lazy val primitiveType: Parser[DataType] =
    "(?i)string".r ^^^ StringType |
      "(?i)float".r ^^^ FloatType |
      "(?i)(?:int|integer)".r ^^^ IntegerType |
      "(?i)tinyint".r ^^^ ByteType |
      "(?i)(?:short|smallint)".r ^^^ ShortType |
      "(?i)double".r ^^^ DoubleType |
      "(?i)(?:long|bigint)".r ^^^ LongType |
      "(?i)binary".r ^^^ BinaryType |
      "(?i)(?:bool|boolean)".r ^^^ BooleanType |
      fixedDecimalType |
      "(?i)decimal".r ^^^ DecimalType.USER_DEFAULT |
      "(?i)date".r ^^^ DateType |
      "(?i)timestamp".r ^^^ TimestampType |
      varchar |
      "(?i)byte".r ^^^ ByteType

  protected lazy val tableCol: Parser[(String, String)] =
    ident ~ primitiveType ^^ {
      case e1 ~ e2 => (e1, e2.toString.dropRight(4).toUpperCase)
    }

  private def toNarrowestIntegerType(value: String): Any = {
    val bigIntValue = BigDecimal(value)

    bigIntValue match {
      case v if bigIntValue.isValidInt => v.toIntExact
      case v if bigIntValue.isValidLong => v.toLongExact
      case v => v.underlying()
    }
  }

  protected lazy val mappings: Parser[Seq[(String, String)]] = repsep(mapping, ",")

  protected lazy val mapping: Parser[(String, String)] =
    ident ~ ("=" ~> ident) ^^ {
      case e1 ~ e2 => (e1, e2)
    }

  protected lazy val nameSpace: Parser[String] = ident <~ "."

  protected lazy val tableCols: Parser[Seq[(String, String)]] = repsep(tableCol, ",")

  protected lazy val keys: Parser[Seq[String]] = repsep(ident, ",")

  protected lazy val values: Parser[Seq[Literal]] = repsep(literal, ",")

  def parse(input: String): LogicalPlan = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError => {
        SqlParser.parse(input)
      }
    }
  }

  // Copy from Spark code, due to unable to leverage SqlParser
  private def toDecimalOrDouble(value: String): Any = {
    val decimal = BigDecimal(value)
    // follow the behavior in MS SQL Server
    // https://msdn.microsoft.com/en-us/library/ms179899.aspx
    if (value.contains('E') || value.contains('e')) {
      decimal.doubleValue()
    } else {
      decimal.underlying()
    }
  }

  protected lazy val literal: Parser[Literal] =
    ( numericLiteral
      | booleanLiteral
      | stringLit ^^ { case s => Literal.create(s, StringType) }
      | intervalLiteral
      | NULL ^^^ Literal.create(null, NullType)
      )

  private def intervalUnit(unitName: String) = acceptIf {
    case lexical.Identifier(str) =>
      val normalized = lexical.normalizeKeyword(str)
      normalized == unitName || normalized == unitName + "s"
    case _ => false
  } {_ => "wrong interval unit"}

  protected lazy val intervalLiteral: Parser[Literal] =
    ( INTERVAL ~> stringLit <~ intervalKeyword("year") ~ intervalKeyword("to") ~
      intervalKeyword("month") ^^ { case s =>
      Literal(CalendarInterval.fromYearMonthString(s))
    }
      | INTERVAL ~> stringLit <~ intervalKeyword("day") ~ intervalKeyword("to") ~
      intervalKeyword("second") ^^ { case s =>
      Literal(CalendarInterval.fromDayTimeString(s))
    }
      | INTERVAL ~> stringLit <~ intervalKeyword("year") ^^ { case s =>
      Literal(CalendarInterval.fromSingleUnitString("year", s))
    }
      | INTERVAL ~> stringLit <~ intervalKeyword("month") ^^ { case s =>
      Literal(CalendarInterval.fromSingleUnitString("month", s))
    }
      | INTERVAL ~> stringLit <~ intervalKeyword("day") ^^ { case s =>
      Literal(CalendarInterval.fromSingleUnitString("day", s))
    }
      | INTERVAL ~> stringLit <~ intervalKeyword("hour") ^^ { case s =>
      Literal(CalendarInterval.fromSingleUnitString("hour", s))
    }
      | INTERVAL ~> stringLit <~ intervalKeyword("minute") ^^ { case s =>
      Literal(CalendarInterval.fromSingleUnitString("minute", s))
    }
      | INTERVAL ~> stringLit <~ intervalKeyword("second") ^^ { case s =>
      Literal(CalendarInterval.fromSingleUnitString("second", s))
    }
      | INTERVAL ~> year.? ~ month.? ~ week.? ~ day.? ~ hour.? ~ minute.? ~ second.? ~
      millisecond.? ~ microsecond.? ^^ { case year ~ month ~ week ~ day ~ hour ~ minute ~ second ~
      millisecond ~ microsecond =>
      if (!Seq(year, month, week, day, hour, minute, second,
        millisecond, microsecond).exists(_.isDefined)) {
        throw new AnalysisException(
          "at least one time unit should be given for interval literal")
      }
      val months = Seq(year, month).map(_.getOrElse(0)).sum
      val microseconds = Seq(week, day, hour, minute, second, millisecond, microsecond)
        .map(_.getOrElse(0L)).sum
      Literal(new CalendarInterval(months, microseconds))
    }
      )

  protected lazy val month: Parser[Int] =
    integral <~ intervalUnit("month") ^^ { case num => num.toInt }

  protected lazy val year: Parser[Int] =
    integral <~ intervalUnit("year") ^^ { case num => num.toInt * 12 }

  protected lazy val microsecond: Parser[Long] =
    integral <~ intervalUnit("microsecond") ^^ { case num => num.toLong }

  protected lazy val millisecond: Parser[Long] =
    integral <~ intervalUnit("millisecond") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_MILLI
    }

  protected lazy val second: Parser[Long] =
    integral <~ intervalUnit("second") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_SECOND
    }

  protected lazy val minute: Parser[Long] =
    integral <~ intervalUnit("minute") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_MINUTE
    }

  protected lazy val hour: Parser[Long] =
    integral <~ intervalUnit("hour") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_HOUR
    }

  protected lazy val day: Parser[Long] =
    integral <~ intervalUnit("day") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_DAY
    }

  protected lazy val week: Parser[Long] =
    integral <~ intervalUnit("week") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_WEEK
    }

  private def intervalKeyword(keyword: String) = acceptIf {
    case lexical.Identifier(str) =>
      lexical.normalizeKeyword(str) == keyword
    case _ => false
  } {_ => "wrong interval keyword"}

  protected lazy val sign: Parser[String] = "+" | "-"
  protected lazy val integral: Parser[String] =
    sign.? ~ numericLit ^^ { case s ~ n => s.getOrElse("") + n }

  protected lazy val numericLiteral: Parser[Literal] =
    ( integral  ^^ { case i => Literal(toNarrowestIntegerType(i)) }
      | sign.? ~ unsignedFloat ^^
      { case s ~ f => Literal(toDecimalOrDouble(s.getOrElse("") + f)) }
      )

  protected lazy val booleanLiteral: Parser[Literal] =
    ( TRUE ^^^ Literal.create(true, BooleanType)
      | FALSE ^^^ Literal.create(false, BooleanType)
      )

  protected lazy val unsignedFloat: Parser[String] =
    ( "." ~> numericLit ^^ { u => "0." + u }
      | elem("decimal", _.isInstanceOf[lexical.DecimalLit]) ^^ (_.chars)
      )
}
