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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Get, HTable, Put, Result, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, _}
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hbase.catalyst.NotPusher
import org.apache.spark.sql.hbase.catalyst.expressions.PartialPredicateOperations.partialPredicateReducer
import org.apache.spark.sql.hbase.types.Range
import org.apache.spark.sql.hbase.util._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, RelationProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class HBaseSource extends RelationProvider {
  // Returns a new HBase relation with the given parameters
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val catalog = sqlContext.catalog.asInstanceOf[HBaseCatalog]

    val tableName = parameters("tableName")
    val rawNamespace = parameters("namespace")
    val hbaseTable = parameters("hbaseTableName")
    val encodingFormat = parameters("encodingFormat")
    val colsSeq = parameters("colsSeq").split(",")
    val keyCols = parameters("keyCols").split(";")
      .map { case c => val cols = c.split(","); (cols(0), cols(1))}
    val nonKeyCols = parameters("nonKeyCols").split(";")
      .filterNot(_ == "")
      .map { case c => val cols = c.split(","); (cols(0), cols(1), cols(2), cols(3))}

    val keyMap: Map[String, String] = keyCols.toMap
    val allColumns = colsSeq.map {
      case name =>
        if (keyMap.contains(name)) {
          KeyColumn(
            name,
            catalog.getDataType(keyMap.get(name).get),
            keyCols.indexWhere(_._1 == name))
        } else {
          val nonKeyCol = nonKeyCols.find(_._1 == name).get
          NonKeyColumn(
            name,
            catalog.getDataType(nonKeyCol._2),
            nonKeyCol._3,
            nonKeyCol._4
          )
        }
    }
    catalog.createTable(tableName, rawNamespace, hbaseTable, allColumns, null, encodingFormat)
  }
}

/**
 *
 * @param tableName SQL table name
 * @param hbaseNamespace physical HBase table namespace
 * @param hbaseTableName physical HBase table name
 * @param allColumns schema
 * @param context HBaseSQLContext
 */
@SerialVersionUID(15298736227428789L)
private[hbase] case class HBaseRelation(
                                         tableName: String,
                                         hbaseNamespace: String,
                                         hbaseTableName: String,
                                         allColumns: Seq[AbstractColumn],
                                         deploySuccessfully: Option[Boolean],
                                         encodingFormat: String = "binaryformat")
                                       (@transient var context: SQLContext)
  extends BaseRelation with InsertableRelation with Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  @transient lazy val keyColumns = allColumns.filter(_.isInstanceOf[KeyColumn])
    .asInstanceOf[Seq[KeyColumn]].sortBy(_.order)

  // The sorting is by the ordering of the Column Family and Qualifier. This is for avoidance
  // to sort cells per row, as needed in bulk loader
  @transient lazy val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
    .asInstanceOf[Seq[NonKeyColumn]].sortWith(
      (a: NonKeyColumn, b: NonKeyColumn) => {
        val empty = new HBaseRawType(0)
        KeyValue.COMPARATOR.compare(
          new KeyValue(empty, a.familyRaw, a.qualifierRaw),
          new KeyValue(empty, b.familyRaw, b.qualifierRaw)) < 0
      }
    )

  @transient lazy val bytesUtils: BytesUtils = encodingFormat match {
    case "stringformat" => StringBytesUtils
    case _ => BinaryBytesUtils
  }

  lazy val partitionKeys = keyColumns.map(col =>
    logicalRelation.output.find(_.name == col.sqlName).get)

  @transient lazy val columnMap = allColumns.map {
    case key: KeyColumn => (key.sqlName, key.order)
    case nonKey: NonKeyColumn => (nonKey.sqlName, nonKey)
  }.toMap

  allColumns.zipWithIndex.foreach(pi => pi._1.ordinal = pi._2)

  private var serializedConfiguration: Array[Byte] = _

  def setConfig(inconfig: Configuration) = {
    config = inconfig
    if (inconfig != null) {
      serializedConfiguration = Util.serializeHBaseConfiguration(inconfig)
    }
  }

  @transient var config: Configuration = _

  private def getConf: Configuration = {
    if (config == null) {
      config = {
        if (serializedConfiguration != null) {
          Util.deserializeHBaseConfiguration(serializedConfiguration)
        }
        else {
          HBaseConfiguration.create
        }
      }
    }
    config
  }

  logger.debug(s"HBaseRelation config has zkPort="
    + s"${getConf.get("hbase.zookeeper.property.clientPort")}")

  @transient private var htable_ : HTable = _

  def htable = {
    if (htable_ == null) htable_ = new HTable(getConf, hbaseTableName)
    htable_
  }

  def isNonKey(attr: AttributeReference): Boolean = {
    keyIndex(attr) < 0
  }

  def keyIndex(attr: AttributeReference): Int = {
    // -1 if nonexistent
    partitionKeys.indexWhere(_.exprId == attr.exprId)
  }

  // find the index in a sequence of AttributeReferences that is a key; -1 if not present
  def rowIndex(refs: Seq[Attribute], keyIndex: Int): Int = {
    refs.indexWhere(_.exprId == partitionKeys(keyIndex).exprId)
  }

  def flushHTable() = {
    if (htable_ != null) {
      htable_.flushCommits()
    }
  }

  def closeHTable() = {
    if (htable_ != null) {
      htable_.close()
      htable_ = null
    }
  }

  // corresponding logical relation
  @transient lazy val logicalRelation = LogicalRelation(this)

  lazy val output = logicalRelation.output

  @transient lazy val dts: Seq[DataType] = allColumns.map(_.dataType)

  /**
   * partitions are updated per table lookup to keep the info reasonably updated
   */
  @transient lazy val partitionExpiration =
    context.conf.asInstanceOf[HBaseSQLConf].partitionExpiration * 1000
  @transient var partitionTS: Long = _

  private[hbase] def fetchPartitions(): Unit = {
    if (System.currentTimeMillis - partitionTS >= partitionExpiration) {
      partitionTS = System.currentTimeMillis
      partitions = {
        val regionLocations = htable.getRegionLocations.asScala.toSeq
        logger.info(s"Number of HBase regions for " +
          s"table ${htable.getName.getNameAsString}: ${regionLocations.size}")
        regionLocations.zipWithIndex.map {
          case p =>
            val start: Option[HBaseRawType] = {
              if (p._1._1.getStartKey.isEmpty) {
                None
              } else {
                Some(p._1._1.getStartKey)
              }
            }
            val end: Option[HBaseRawType] = {
              if (p._1._1.getEndKey.isEmpty) {
                None
              } else {
                Some(p._1._1.getEndKey)
              }
            }
            new HBasePartition(
              p._2, p._2,
              start,
              end,
              Some(p._1._2.getHostname), relation = this)
        }
      }
    }
  }

  @transient var partitions: Seq[HBasePartition] = _

  @transient private[hbase] lazy val dimSize = keyColumns.size

  val scannerFetchSize = context.conf.asInstanceOf[HBaseSQLConf].scannerFetchSize

  private[hbase] def generateRange(partition: HBasePartition, pred: Expression,
                                   index: Int): Range[_] = {
    def getData(dt: AtomicType, bound: Option[HBaseRawType]): Option[Any] = {
      if (bound.isEmpty) {
        None
      } else {
        /**
         * the partition start/end could be incomplete byte array, so we need to make it
         * a complete key first
         */
        val finalRowKey = getFinalKey(bound)
        val (start, length) = HBaseKVHelper.decodingRawKeyColumns(finalRowKey, keyColumns)(index)
        Some(DataTypeUtils.bytesToData(finalRowKey, start, length, dt).asInstanceOf[dt.InternalType])
      }
    }

    val dt = keyColumns(index).dataType.asInstanceOf[AtomicType]
    val isLastKeyIndex = index == (keyColumns.size - 1)
    val start = getData(dt, partition.start)
    val end = getData(dt, partition.end)
    val startInclusive = start.nonEmpty
    val endInclusive = end.nonEmpty && !isLastKeyIndex
    new Range(start, startInclusive, end, endInclusive, dt)
  }


  /**
   * Return the start keys of all of the regions in this table,
   * as a list of SparkImmutableBytesWritable.
   */
  def getRegionStartKeys = {
    val byteKeys: Array[HBaseRawType] = htable.getStartKeys
    val ret = ArrayBuffer[HBaseRawType]()

    // Since the size of byteKeys will be 1 if there is only one partition in the table,
    // we need to omit that null element.
    for (byteKey <- byteKeys if !(byteKeys.length == 1 && byteKeys(0).length == 0)) {
      ret += byteKey
    }

    ret
  }

  /**
   * build filter list based on critical point ranges
   * @param output the projection list
   * @param filterPred the predicate
   * @param cprs the sequence of critical point ranges
   * @return the filter list and expression tuple
   */
  def buildCPRFilterList(output: Seq[Attribute], filterPred: Option[Expression],
                         cprs: Seq[MDCriticalPointRange[_]]):
  (Option[Filter], Option[Expression]) = {
    val cprFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
    var expressionList: List[Expression] = List[Expression]()
    var anyNonpushable = false
    for (cpr <- cprs) {
      val cprAndPushableFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      val startKey: Option[Any] = cpr.lastRange.start
      val endKey: Option[Any] = cpr.lastRange.end
      val startInclusive = cpr.lastRange.startInclusive
      val endInclusive = cpr.lastRange.endInclusive
      val keyType: AtomicType = cpr.lastRange.dt
      val predicate = Option(cpr.lastRange.pred)
      val (pushable, nonPushable) = buildPushdownFilterList(predicate)

      val items: Seq[(Any, AtomicType)] = cpr.prefix
      val head: Seq[(HBaseRawType, AtomicType)] = items.map {
        case (itemValue, itemType) =>
          (DataTypeUtils.dataToBytes(itemValue, itemType), itemType)
      }

      val headExpression: Seq[Expression] = items.zipWithIndex.map { case (item, index) =>
        val keyCol = keyColumns.find(_.order == index).get

        val left = filterPred.get.references.find(_.name == keyCol.sqlName).get
        val right = Literal.create(item._1, item._2)
        EqualTo(left, right)
      }

      val tailExpression: Expression = {
        val index = items.size
        val keyCol = keyColumns.find(_.order == index).get
        val left = filterPred.get.references.find(_.name == keyCol.sqlName).get
        val startInclusive = cpr.lastRange.startInclusive
        val endInclusive = cpr.lastRange.endInclusive
        if (cpr.lastRange.isPoint) {
          val right = Literal.create(cpr.lastRange.start.get, cpr.lastRange.dt)
          EqualTo(left, right)
        } else if (cpr.lastRange.start.isDefined && cpr.lastRange.end.isDefined) {
          var right = Literal.create(cpr.lastRange.start.get, cpr.lastRange.dt)
          val leftExpression = if (startInclusive) {
            GreaterThanOrEqual(left, right)
          } else {
            GreaterThan(left, right)
          }
          right = Literal.create(cpr.lastRange.end.get, cpr.lastRange.dt)
          val rightExpress = if (endInclusive) {
            LessThanOrEqual(left, right)
          } else {
            LessThan(left, right)
          }
          And(leftExpression, rightExpress)
        } else if (cpr.lastRange.start.isDefined) {
          val right = Literal.create(cpr.lastRange.start.get, cpr.lastRange.dt)
          if (startInclusive) {
            GreaterThanOrEqual(left, right)
          } else {
            GreaterThan(left, right)
          }
        } else if (cpr.lastRange.end.isDefined) {
          val right = Literal.create(cpr.lastRange.end.get, cpr.lastRange.dt)
          if (endInclusive) {
            LessThanOrEqual(left, right)
          } else {
            LessThan(left, right)
          }
        } else {
          null
        }
      }

      val combinedExpression: Seq[Expression] = headExpression :+ tailExpression
      var andExpression: Expression = combinedExpression.reduceLeft(
        (e1: Expression, e2: Expression) => And(e1, e2))

      if (nonPushable.isDefined) {
        anyNonpushable = true
        andExpression = And(andExpression, nonPushable.get)
      }
      expressionList = expressionList :+ andExpression

      val filter = {
        if (cpr.lastRange.isPoint) {
          // the last range is a point
          val tail: (HBaseRawType, AtomicType) =
            (DataTypeUtils.dataToBytes(startKey.get, keyType), keyType)
          val rowKeys = head :+ tail
          val row = HBaseKVHelper.encodingRawKeyColumns(rowKeys)
          if (cpr.prefix.size == keyColumns.size - 1) {
            // full dimension of row key
            new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(row))
          }
          else {
            new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(row))
          }
        } else {
          // the last range is not a point
          val startFilter: RowFilter = if (startKey.isDefined) {
            val tail: (HBaseRawType, AtomicType) =
              (DataTypeUtils.dataToBytes(startKey.get, keyType), keyType)
            val rowKeys = head :+ tail
            val row = HBaseKVHelper.encodingRawKeyColumns(rowKeys)
            if (cpr.prefix.size == keyColumns.size - 1) {
              // full dimension of row key
              if (startInclusive) {
                new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(row))
              } else {
                new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(row))
              }
            }
            else {
              if (startInclusive) {
                new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                  new BinaryPrefixComparator(row))
              } else {
                new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryPrefixComparator(row))
              }
            }
          } else {
            null
          }
          val endFilter: RowFilter = if (endKey.isDefined) {
            val tail: (HBaseRawType, AtomicType) =
              (DataTypeUtils.dataToBytes(endKey.get, keyType), keyType)
            val rowKeys = head :+ tail
            val row = HBaseKVHelper.encodingRawKeyColumns(rowKeys)
            if (cpr.prefix.size == keyColumns.size - 1) {
              // full dimension of row key
              if (endInclusive) {
                new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(row))
              } else {
                new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(row))
              }
            } else {
              if (endInclusive) {
                new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                  new BinaryPrefixComparator(row))
              } else {
                new RowFilter(CompareFilter.CompareOp.LESS, new BinaryPrefixComparator(row))
              }
            }
          } else {
            null
          }
          /*
          * create the filter, for example, k1 = 10, k2 < 5
          * it will create 2 filters, first RowFilter = 10 (PrefixComparator),
          * second, RowFilter < (10, 5) (PrefixComparator / Comparator)
          */
          val prefixFilter = if (head.nonEmpty) {
            val row = HBaseKVHelper.encodingRawKeyColumns(head)
            new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(row))
          } else {
            null
          }
          if (startKey.isDefined && endKey.isDefined) {
            // both start and end filters exist
            val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
            if (prefixFilter != null) {
              filterList.addFilter(prefixFilter)
            }
            filterList.addFilter(startFilter)
            filterList.addFilter(endFilter)
            filterList
          } else if (startKey.isDefined) {
            // start filter exists only
            if (prefixFilter != null) {
              val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
              filterList.addFilter(prefixFilter)
              filterList.addFilter(startFilter)
              filterList
            } else {
              startFilter
            }
          } else {
            // end filter exists only
            if (prefixFilter != null) {
              val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
              filterList.addFilter(prefixFilter)
              filterList.addFilter(endFilter)
              filterList
            } else {
              endFilter
            }
          }
        }
      }
      cprAndPushableFilterList.addFilter(filter)
      if (pushable.isDefined) {
        cprAndPushableFilterList.addFilter(pushable.get)
      }
      cprFilterList.addFilter(cprAndPushableFilterList)
    }

    val orExpression = if (anyNonpushable) {
      Some(expressionList.reduceLeft((e1: Expression, e2: Expression) => Or(e1, e2)))
    } else {
      None
    }
    val finalFilterList: Filter = if (cprFilterList.getFilters.size() == 1) {
      cprFilterList.getFilters.get(0)
    } else if (cprFilterList.getFilters.size() > 1) {
      cprFilterList
    } else {
      require(requirement = false, "internal logic error: nonempty filter list is expected")
      null
    }
    (Some(finalFilterList), orExpression)
  }

  /**
   * create pushdown filter list based on predicate
   * @param pred the predicate
   * @return tuple(filter list, non-pushdownable expression, pushdown predicates)
   */
  def buildPushdownFilterList(pred: Option[Expression]):
  (Option[FilterList], Option[Expression]) = {
    if (pred.isDefined) {
      val predExp: Expression = pred.get
      // build pred pushdown filters:
      // 1. push any NOT through AND/OR
      val notPushedPred = NotPusher(predExp)
      // 2. classify the transformed predicate into pushdownable and non-pushdownable predicates
      val classier = new ScanPredClassifier(this) // Right now only on primary key dimension
      val (pushdownFilterPred, otherPred) = classier(notPushedPred)
      // 3. build a FilterList mirroring the pushdownable predicate
      val predPushdownFilterList = {
        if (pushdownFilterPred.isEmpty) None else buildFilterListFromPred(pushdownFilterPred)
      }
      // 4. merge the above FilterList with the one from the projection
      (predPushdownFilterList, otherPred)
    } else {
      (None, None)
    }
  }

  /**
   * add the filter to the filter list
   * @param filters the filter list
   * @param filtersToBeAdded the filter to be added
   * @param operator the operator of the filter to be added
   */
  private def addToFilterList(filters: java.util.ArrayList[Filter],
                              filtersToBeAdded: Option[FilterList],
                              operator: FilterList.Operator) = {
    if (filtersToBeAdded.isDefined) {
      val filterList = filtersToBeAdded.get
      val size = filterList.getFilters.size
      if (size == 1 || filterList.getOperator == operator) {
        filterList.getFilters.map(p => filters.add(p))
      } else {
        filters.add(filterList)
      }
    }
  }

  def createSingleColumnValueFilter(left: AttributeReference, right: Literal,
                                    compareOp: CompareFilter.CompareOp): Option[FilterList] = {
    val nonKeyColumn = nonKeyColumns.find(_.sqlName == left.name)
    if (nonKeyColumn.isDefined) {
      val column = nonKeyColumn.get
      val filter = new SingleColumnValueFilter(column.familyRaw,
        column.qualifierRaw,
        compareOp,
        DataTypeUtils.getBinaryComparator(bytesUtils.create(right.dataType), right))
      filter.setFilterIfMissing(true)
      Some(new FilterList(filter))
    } else {
      None
    }
  }

  /**
   * recursively create the filter list based on predicate
   * @param pred the predicate
   * @return the filter list, or None if predicate is not defined
   */
  private def buildFilterListFromPred(pred: Option[Expression]): Option[FilterList] = {
    if (pred.isEmpty) {
      None
    } else {
      val expression = pred.get
      expression match {
        case And(left, right) =>
          val filters = new java.util.ArrayList[Filter]
          if (left != null) {
            val leftFilterList = buildFilterListFromPred(Some(left))
            addToFilterList(filters, leftFilterList, FilterList.Operator.MUST_PASS_ALL)
          }
          if (right != null) {
            val rightFilterList = buildFilterListFromPred(Some(right))
            addToFilterList(filters, rightFilterList, FilterList.Operator.MUST_PASS_ALL)
          }
          Some(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters))
        case Or(left, right) =>
          val filters = new java.util.ArrayList[Filter]
          if (left != null) {
            val leftFilterList = buildFilterListFromPred(Some(left))
            addToFilterList(filters, leftFilterList, FilterList.Operator.MUST_PASS_ONE)
          }
          if (right != null) {
            val rightFilterList = buildFilterListFromPred(Some(right))
            addToFilterList(filters, rightFilterList, FilterList.Operator.MUST_PASS_ONE)
          }
          Some(new FilterList(FilterList.Operator.MUST_PASS_ONE, filters))
        case InSet(value@AttributeReference(name, dataType, _, _), hset) =>
          val column = nonKeyColumns.find(_.sqlName == name)
          if (column.isDefined) {
            val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
            for (item <- hset) {
              val filter = new SingleColumnValueFilter(column.get.familyRaw,
                column.get.qualifierRaw,
                CompareFilter.CompareOp.EQUAL,
                DataTypeUtils.getBinaryComparator(bytesUtils.create(dataType),
                  Literal.create(item, dataType)))
              filterList.addFilter(filter)
            }
            Some(filterList)
          } else {
            None
          }
        case In(value@AttributeReference(name, dataType, _, _), list) =>
          val column = nonKeyColumns.find(_.sqlName == name)
          if (column.isDefined) {
            val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
            for (item <- list) {
              val filter = new SingleColumnValueFilter(column.get.familyRaw,
                column.get.qualifierRaw,
                CompareFilter.CompareOp.EQUAL,
                DataTypeUtils.getBinaryComparator(bytesUtils.create(dataType),
                  item.asInstanceOf[Literal]))
              filterList.addFilter(filter)
            }
            Some(filterList)
          } else {
            None
          }
        case GreaterThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.GREATER)
        case GreaterThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.GREATER)
        case GreaterThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right,
            CompareFilter.CompareOp.GREATER_OR_EQUAL)
        case GreaterThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left,
            CompareFilter.CompareOp.GREATER_OR_EQUAL)
        case EqualTo(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.EQUAL)
        case EqualTo(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.EQUAL)
        case LessThan(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.LESS)
        case LessThan(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.LESS)
        case LessThanOrEqual(left: AttributeReference, right: Literal) =>
          createSingleColumnValueFilter(left, right, CompareFilter.CompareOp.LESS_OR_EQUAL)
        case LessThanOrEqual(left: Literal, right: AttributeReference) =>
          createSingleColumnValueFilter(right, left, CompareFilter.CompareOp.LESS_OR_EQUAL)
        case _ => None
      }
    }
  }

  def buildPut(row: InternalRow): Put = {
    // TODO: revisit this using new KeyComposer
    val rowKey: HBaseRawType = null
    new Put(rowKey)
  }

  def sqlContext = context

  def schema: StructType = StructType(allColumns.map {
    case KeyColumn(name, dt, _) => StructField(name, dt, nullable = false)
    case NonKeyColumn(name, dt, _, _) => StructField(name, dt, nullable = true)
  })

  override def insert(data: DataFrame, overwrite: Boolean) = {
    if (!overwrite) {
      sqlContext.sparkContext.runJob(data.rdd, writeToHBase _)
    } else {
      // TODO: Support INSERT OVERWRITE INTO
      sys.error("HBASE Table does not support INSERT OVERWRITE for now.")
    }
  }

  def writeToHBase(context: TaskContext, iterator: Iterator[Row]) = {
    // TODO:make the BatchMaxSize configurable
    val BatchMaxSize = 100

    var rowIndexInBatch = 0
    var colIndexInBatch = 0

    var puts = new ListBuffer[Put]()
    while (iterator.hasNext) {
      val row = iterator.next()
      val seq = row.toSeq.map {
        case s: String => UTF8String.fromString(s)
        case other => other
      }
      val internalRow = InternalRow.fromSeq(seq)
      val rawKeyCol = keyColumns.map(
        kc => {
          val rowColumn = DataTypeUtils.getRowColumnInHBaseRawType(
            internalRow, kc.ordinal, kc.dataType)
          colIndexInBatch += 1
          (rowColumn, kc.dataType)
        }
      )
      val key = HBaseKVHelper.encodingRawKeyColumns(rawKeyCol)
      val put = new Put(key)
      nonKeyColumns.foreach(
        nkc => {
          val rowVal = DataTypeUtils.getRowColumnInHBaseRawType(
            internalRow, nkc.ordinal, nkc.dataType, bytesUtils)
          colIndexInBatch += 1
          put.add(nkc.familyRaw, nkc.qualifierRaw, rowVal)
        }
      )

      puts += put
      colIndexInBatch = 0
      rowIndexInBatch += 1
      if (rowIndexInBatch >= BatchMaxSize) {
        htable.put(puts.toList)
        puts.clear()
        rowIndexInBatch = 0
      }
    }
    if (puts.nonEmpty) {
      htable.put(puts.toList)
    }
    closeHTable()
  }


  def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[InternalRow] = {
    require(filters.size < 2, "Internal logical error: unexpected filter list size")
    val filterPredicate = filters.headOption
    new HBaseSQLReaderRDD(
      this,
      context.conf.codegenEnabled,
      context.conf.asInstanceOf[HBaseSQLConf].useCustomFilter,
      requiredColumns,
      subplan = None,
      dummyRDD = null,
      deploySuccessfully,
      filterPredicate, // PartitionPred : Option[Expression]
      context
    )
  }

  def buildScan(start: Option[HBaseRawType], end: Option[HBaseRawType],
                predicate: Option[Expression],
                filters: Option[Filter], otherFilters: Option[Expression],
                useCustomFilter: Boolean,
                projectionList: Seq[NamedExpression]): Scan = {
    val scan = {
      (start, end) match {
        case (Some(lb), Some(ub)) => new Scan(lb, ub)
        case (Some(lb), None) => new Scan(lb)
        case (None, Some(ub)) => new Scan(Array[Byte](), ub)
        case _ => new Scan
      }
    }

    // set fetch size
    scan.setCaching(scannerFetchSize)

    // add Family to SCAN from projections
    addColumnFamiliesToScan(scan, filters, otherFilters,
      predicate, useCustomFilter, projectionList)
  }

  /**
   * add projection and column to the scan
   * @param scan the current scan
   * @param filters the filter/filter list to be processed
   * @param otherFilters the non-pushdownable predicates
   * @param projectionList the projection list
   * @return the proper scan
   */
  def addColumnFamiliesToScan(scan: Scan, filters: Option[Filter],
                              otherFilters: Option[Expression],
                              predicate: Option[Expression],
                              useCustomFilter: Boolean,
                              projectionList: Seq[NamedExpression]): Scan = {
    var distinctProjectionList = projectionList.map(_.name)
    var keyOnlyFilterPresent = false
    if (otherFilters.isDefined) {
      distinctProjectionList =
        distinctProjectionList.union(otherFilters.get.references.toSeq.map(_.name)).distinct
    }
    // filter out the key columns
    distinctProjectionList =
      distinctProjectionList.filterNot(p => keyColumns.exists(_.sqlName == p))

    var finalFilters = if (distinctProjectionList.isEmpty) {
      if (filters.isDefined) {
        filters.get
      } else {
        keyOnlyFilterPresent = true
        new FirstKeyOnlyFilter
      }
    } else {
      if (filters.isDefined) {
        filters.get
      } else {
        null
      }
    }

    if (predicate.nonEmpty) {
      val pred = predicate.get
      val predRefs = pred.references.toSeq
      val predicateNameSet = predRefs.map(_.name).
        filterNot(p => keyColumns.exists(_.sqlName == p)).toSet
      if (distinctProjectionList.toSet.subsetOf(predicateNameSet)) {
        // If the pushed down predicate is present and the projection is a subset
        // of the columns of the pushed filters, use the columns as projections
        // to avoid a full projection. The point is that by default without
        // adding column explicitly, the HBase scan would return all columns.
        // However there is some complexity when the predicate involves checks against
        // nullness. For instance, "SELECT c1 from ... where c2 is null" would require
        // the full projection before a check can be made since any existence of
        // any other column would qualify the row. In contrast, a query of
        // "SELECT c2 from ... where c2 is not null" will only require the existence
        // of c2 so we can restrict the interested qualifiers to "c2" only.
        distinctProjectionList = predicateNameSet.toSeq.distinct
        val boundPred = BindReferences.bindReference(pred, predRefs)
        val row = new GenericInternalRow(predRefs.size) // an all-null row
        val prRes = boundPred.partialReduce(row, predRefs, checkNull = true)
        val (addColumn, nkcols) = prRes match {
          //  At least one existing column has to be fetched to qualify the record,
          //  so we can just use the predicate's full projection
          case (false, _) => (true, distinctProjectionList)

          // Even an absent column may qualify the record, we have to fetch
          // all columns before evaluate the predicate. Note that by doing this, the "fullness"
          // of projection has a semantic scope of the HBase table, not the SQL table
          // mapped to the HBase table.
          case (true, _) => (false, null)

          // Any and all absent columns aren't enough to determine the record qualification,
          // so the 'remaining' prdeicate's projections have to be consulted and we
          // can avoid full projection again by adding the 'remaining' prdeicate's projections
          // to the scan's column map if the projections are non-key columns
          case (null, reducedPred) =>
            val nkRefs = reducedPred.references.map(_.name).filterNot(
              p => keyColumns.exists(_.sqlName == p))
            if (nkRefs.isEmpty) {
              // Only key-related predicate is present, add FirstKeyOnlyFilter
              if (!keyOnlyFilterPresent) {
                if (finalFilters == null) {
                  finalFilters = new FirstKeyOnlyFilter
                } else {
                  val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
                  filterList.addFilter(new FirstKeyOnlyFilter)
                  filterList.addFilter(finalFilters)
                  finalFilters = filterList
                }
              }
              (false, null)
            } else {
              (true, nkRefs.toSeq)
            }
        }
        if (addColumn && nkcols.nonEmpty && nkcols.size < nonKeyColumns.size) {
          nkcols.foreach {
            case p =>
              val nkc = nonKeyColumns.find(_.sqlName == p).get
              scan.addColumn(nkc.familyRaw, nkc.qualifierRaw)
          }
        }
      }
    }

    if (deploySuccessfully.isDefined && deploySuccessfully.get && useCustomFilter) {
      if (finalFilters != null) {
        if (otherFilters.isDefined) {
          // add custom filter to handle other filters part
          val customFilter = new HBaseCustomFilter(this, otherFilters.get)
          val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
          filterList.addFilter(finalFilters)
          filterList.addFilter(customFilter)
          scan.setFilter(filterList)
        } else {
          scan.setFilter(finalFilters)
        }
      } else if (otherFilters.isDefined) {
        val customFilter = new HBaseCustomFilter(this, otherFilters.get)
        scan.setFilter(customFilter)
      }
    } else {
      if (finalFilters != null) scan.setFilter(finalFilters)
    }
    scan
  }

  def buildGet(projectionList: Seq[NamedExpression], rowKey: HBaseRawType) {
    new Get(rowKey)
    // TODO: add columns to the Get
  }

  /**
   *
   * @param kv the cell value to work with
   * @param projection the pair of projection and its index
   * @param row the row to set values on
   */
  private def setColumn(kv: Cell, projection: (Attribute, Int), row: MutableRow,
                        bytesUtils: BytesUtils = BinaryBytesUtils): Unit = {
    if (kv == null || kv.getValueLength == 0) {
      row.setNullAt(projection._2)
    } else {
      val dt = projection._1.dataType
      if (dt.isInstanceOf[AtomicType]) {
        DataTypeUtils.setRowColumnFromHBaseRawType(
          row, projection._2, kv.getValueArray, kv.getValueOffset, kv.getValueLength, dt, bytesUtils)
      } else {
        // for complex types, deserialiation is involved and we aren't sure about buffer safety
        val colValue = CellUtil.cloneValue(kv)
        DataTypeUtils.setRowColumnFromHBaseRawType(
          row, projection._2, colValue, 0, colValue.length, dt, bytesUtils)
      }
    }
  }

  def buildRowAfterCoprocessor(projections: Seq[(Attribute, Int)],
                               result: Result,
                               row: MutableRow): InternalRow = {
    for (i <- projections.indices) {
      setColumn(result.rawCells()(i), projections(i), row)
    }
    row
  }

  def buildRowInCoprocessor(projections: Seq[(Attribute, Int)],
                            result: java.util.ArrayList[Cell],
                            row: MutableRow): InternalRow = {
    def getColumnLatestCell(family: Array[Byte],
                            qualifier: Array[Byte]): Cell = {
      // 0 means equal, >0 means larger, <0 means smaller
      def compareCellWithExpectedOne(cell: Cell): Int = {
        val compare = Bytes.compareTo(
          cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength,
          family, 0, family.length)
        if (compare != 0) compare
        else {
          Bytes.compareTo(
            cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength,
            qualifier, 0, qualifier.length)
        }
      }

      def binarySearchTheArrayList(startIndex: Int, endIndex: Int): Cell = {
        if (startIndex > endIndex) null
        else {
          val midIndex = (startIndex + endIndex) >>> 1
          val cell = result.get(midIndex)
          // 0 means equal, >0 means larger, <0 means smaller
          compareCellWithExpectedOne(cell) match {
            case 0 => cell
            case i if i < 0 => binarySearchTheArrayList(midIndex + 1, endIndex)
            case i if i > 0 => binarySearchTheArrayList(startIndex, midIndex - 1)
          }
        }
      }

      if (result == null || result.isEmpty) null
      else {
        binarySearchTheArrayList(0, result.length - 1)
      }
    }

    lazy val rowKeys = HBaseKVHelper.decodingRawKeyColumns(
      result.head.getRowArray, keyColumns, result.head.getRowLength, result.head.getRowOffset)
    projections.foreach {
      p =>
        columnMap.get(p._1.name).get match {
          case column: NonKeyColumn =>
            val kv = getColumnLatestCell(column.familyRaw, column.qualifierRaw)
            setColumn(kv, p, row, bytesUtils)
          case keyIndex: Int =>
            val (start, length) = rowKeys(keyIndex)
            DataTypeUtils.setRowColumnFromHBaseRawType(
              row, p._2, result.head.getRowArray, start, length, keyColumns(keyIndex).dataType)
        }
    }
    row
  }

  def buildRow(projections: Seq[(Attribute, Int)],
               result: Result,
               row: MutableRow): InternalRow = {
    lazy val rowKeys = HBaseKVHelper.decodingRawKeyColumns(result.getRow, keyColumns)
    projections.foreach {
      p =>
        columnMap.get(p._1.name).get match {
          case column: NonKeyColumn =>
            val kv: Cell = result.getColumnLatestCell(column.familyRaw, column.qualifierRaw)
            setColumn(kv, p, row, bytesUtils)
          case keyIndex: Int =>
            val (start, length) = rowKeys(keyIndex)
            DataTypeUtils.setRowColumnFromHBaseRawType(
              row, p._2, result.getRow, start, length, keyColumns(keyIndex).dataType)
        }
    }
    row
  }

  /**
   * Convert the row key to its proper format. Due to the nature of HBase, the start and
   * end of partition could be partial row key, we may need to append 0x00 to make it comply
   * with the definition of key columns, for example, add four 0x00 if a key column type is
   * integer. Also string type (of UTF8) may need to be padded with the minimum UTF8
   * continuation byte(s)
   * @param rawKey the original row key
   * @return the proper row key based on the definition of the key columns
   */
  def getFinalKey(rawKey: Option[HBaseRawType]): HBaseRawType = {
    val origRowKey: HBaseRawType = rawKey.get

    /**
     * Recursively run this function to check the key columns one by one.
     * If the input raw key contains the whole part of this key columns, then continue to
     * check the next one; otherwise, append the raw key by adding 0x00(or minimal UTF8
     * continuation bytes) to its proper format and return it.
     * @param rowIndex the start point of unchecked bytes in the input raw key
     * @param curKeyIndex the next key column need to be checked
     * @return the proper row key based on the definition of the key columns
     */
    def getFinalRowKey(rowIndex: Int, curKeyIndex: Int): HBaseRawType = {
      if (curKeyIndex >= keyColumns.length) origRowKey
      else {
        val typeOfKey = keyColumns(curKeyIndex)
        if (typeOfKey.dataType == StringType) {
          val indexOfStringEnd = origRowKey.indexOf(HBaseKVHelper.delimiter, rowIndex)
          if (indexOfStringEnd == -1) {
            val nOfUTF8StrBytes = HBaseRelation.numOfBytes(origRowKey(rowIndex))
            val delta = if (nOfUTF8StrBytes > origRowKey.length - rowIndex) {
              // padding of 1000 0000 is needed according to UTF-8 spec
              Array.fill[Byte](nOfUTF8StrBytes - origRowKey.length
                + rowIndex)(HBaseRelation.utf8Padding) ++
                new Array[Byte](getMinimum(curKeyIndex + 1))
            } else {
              new Array[Byte](getMinimum(curKeyIndex + 1))
            }
            origRowKey ++ delta
          } else {
            getFinalRowKey(indexOfStringEnd + 1, curKeyIndex + 1)
          }
        } else {
          val nextRowIndex = rowIndex +
            typeOfKey.dataType.asInstanceOf[AtomicType].defaultSize
          if (nextRowIndex < origRowKey.length) {
            getFinalRowKey(nextRowIndex, curKeyIndex + 1)
          } else {
            val delta: Array[Byte] = {
              new Array[Byte](nextRowIndex - origRowKey.length + getMinimum(curKeyIndex + 1))
            }
            origRowKey ++ delta
          }
        }
      }
    }

    /**
     * Get the minimum key length based on the key columns definition
     * @param startKeyIndex the start point of the key column
     * @return the minimum length required for the remaining key columns
     */
    def getMinimum(startKeyIndex: Int): Int = {
      keyColumns.drop(startKeyIndex).map(k => {
        k.dataType match {
          case StringType => 1
          case _ => k.dataType.asInstanceOf[AtomicType].defaultSize
        }
      }
      ).sum
    }

    getFinalRowKey(0, 0)
  }

  /**
   * Convert a HBase row key into column values in their native data formats
   * @param rawKey the HBase row key
   * @return A sequence of column values from the row Key
   */
  def nativeKeyConvert(rawKey: Option[HBaseRawType]): Seq[Any] = {
    if (rawKey.isEmpty) Nil
    else {
      val finalRowKey = getFinalKey(rawKey)

      HBaseKVHelper.decodingRawKeyColumns(finalRowKey, keyColumns).
        zipWithIndex.map(pi => DataTypeUtils.bytesToData(finalRowKey,
        pi._1._1, pi._1._2, keyColumns(pi._2).dataType))
    }
  }
}

private[hbase] object HBaseRelation {
  //  Copied from UTF8String for accessibility reasons therein
  private val bytesOfCodePointInUTF8: Array[Int] = Array(2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5,
    6, 6, 6, 6)

  @inline
  def numOfBytes(b: Byte): Int = {
    val offset = (b & 0xFF) - 192
    if (offset >= 0) bytesOfCodePointInUTF8(offset) else 1
  }

  val zeroByte: Array[Byte] = new Array(1)
  val utf8Padding: Byte = 0x80.asInstanceOf[Byte]
}

