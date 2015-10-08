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
package org.apache.spark.sql.hbase.execution

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.{Job, RecordWriter}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hbase.HBasePartitioner.HBaseRawOrdering
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.util.{DataTypeUtils, Util}
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, SerializableWritable, SparkEnv, TaskContext}

import scala.collection.mutable.ArrayBuffer

@DeveloperApi
case class AlterDropColCommand(tableName: String, columnName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.catalog.asInstanceOf[HBaseCatalog].alterTableDropNonKey(tableName, columnName)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

@DeveloperApi
case class AlterAddColCommand(
                               tableName: String,
                               colName: String,
                               colType: String,
                               colFamily: String,
                               colQualifier: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val hbaseCatalog = sqlContext.catalog.asInstanceOf[HBaseCatalog]
    hbaseCatalog.alterTableAddNonKey(tableName,
      NonKeyColumn(
        colName, hbaseCatalog.getDataType(colType), colFamily, colQualifier)
    )
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

@DeveloperApi
case class DropHbaseTableCommand(tableName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val hbaseCatalog = sqlContext.catalog.asInstanceOf[HBaseCatalog]
    hbaseCatalog.deleteTable(tableName)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

@DeveloperApi
case object ShowTablesCommand extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val buffer = new ArrayBuffer[Row]()
    val tables = sqlContext.catalog.asInstanceOf[HBaseCatalog].getAllTableName
    tables.foreach(x => buffer.append(Row(x)))
    buffer.toSeq
  }

  override def output: Seq[Attribute] = StructType(Seq(StructField("", StringType))).toAttributes
}

@DeveloperApi
case class DescribeTableCommand(tableName: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val buffer = new ArrayBuffer[Row]()
    val relation = sqlContext.catalog.asInstanceOf[HBaseCatalog].getTable(tableName)
    if (relation.isDefined) {
      relation.get.allColumns.foreach {
        case keyColumn: KeyColumn =>
          buffer.append(Row(keyColumn.sqlName, keyColumn.dataType.toString,
            "KEY COLUMN", keyColumn.order.toString))
        case nonKeyColumn: NonKeyColumn =>
          buffer.append(Row(nonKeyColumn.sqlName, nonKeyColumn.dataType.toString,
            "NON KEY COLUMN", nonKeyColumn.family, nonKeyColumn.qualifier))
      }
      buffer.toSeq
    } else {
      sys.error(s"can not find table $tableName")
    }
  }

  override def output: Seq[Attribute] =
    StructType(Seq.fill(5)(StructField("", StringType))).toAttributes
}

@DeveloperApi
case class InsertValueIntoTableCommand(tableName: String, valueSeq: Seq[String])
  extends RunnableCommand {
  override def run(sqlContext: SQLContext) = {
    val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]

    val bytes = valueSeq.zipWithIndex.map(v =>
      DataTypeUtils.string2TypeData(v._1, relation.schema(v._2).dataType))

    val rows = sqlContext.sparkContext.makeRDD(Seq(Row.fromSeq(bytes)))
    val inputValuesDF = sqlContext.createDataFrame(rows, relation.schema)
    relation.insert(inputValuesDF, overwrite = false)

    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty

  // Override the following two functions to solve the problem in inserting a null value
  // Remove this part if you have found a better sollution

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
   * @param rule the rule to be applied to every expression in this operator.
   */
  override def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionDown(e: Expression): Expression = {
      val newE = e.transformDown(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionDown(e)
      case Some(e: Expression) => Some(transformExpressionDown(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null // !!! Important thing to add to handle the null value
    }

    val newArgs = productIterator.map(recursiveTransform).toArray

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  override def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionUp(e: Expression): Expression = {
      val newE = e.transformUp(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionUp(e)
      case Some(e: Expression) => Some(transformExpressionUp(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null // !!! Important thing to add to handle the null value
    }

    val newArgs = productIterator.map(recursiveTransform).toArray

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }
}

@DeveloperApi
case class BulkLoadIntoTableCommand(
                                     inputPath: String,
                                     tableName: String,
                                     isLocal: Boolean,
                                     delimiter: Option[String],
                                     parallel: Boolean)
  extends RunnableCommand
  with SparkHadoopMapReduceUtil
  with Logging {

  override def run(sqlContext: SQLContext) = {
    @transient val solvedRelation = sqlContext.catalog.lookupRelation(Seq(tableName))
    @transient val relation: HBaseRelation = solvedRelation.asInstanceOf[Subquery]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]
    @transient val hbContext = sqlContext.asInstanceOf[HBaseSQLContext]

    // tmp path for storing HFile
    @transient val tmpPath = Util.getTempFilePath(
      hbContext.sparkContext.hadoopConfiguration, relation.tableName)
    @transient val job = Job.getInstance(hbContext.sparkContext.hadoopConfiguration)
    HFileOutputFormat2.configureIncrementalLoad(job, relation.htable)
    job.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", tmpPath)

    @transient val conf = job.getConfiguration

    @transient val hadoopReader = if (isLocal) {
      val fs = FileSystem.getLocal(conf)
      val pathString = fs.pathToFile(new Path(inputPath)).toURI.toURL.toString
      new HadoopReader(sqlContext.sparkContext, pathString, delimiter)(relation)
    } else {
      new HadoopReader(sqlContext.sparkContext, inputPath, delimiter)(relation)
    }

    @transient val splitKeys = relation.getRegionStartKeys.toArray
    @transient val wrappedConf = new SerializableWritable(conf)

    @transient val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    @transient val partitioner = new HBasePartitioner(splitKeys)
    @transient val ordering = Ordering[HBaseRawType]
    @transient val shuffled =
      new HBaseShuffledRDD(rdd, partitioner, relation.partitions).setKeyOrdering(ordering)

    @transient val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    @transient val jobtrackerID = formatter.format(new Date())
    @transient val stageId = shuffled.id
    @transient val jobFormat = new HFileOutputFormat2

    if (SparkEnv.get.conf.getBoolean("spark.hadoop.validateOutputSpecs", defaultValue = true)) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    @transient val par = parallel
    @transient val writeShard =
      (context: TaskContext, iter: Iterator[(HBaseRawType, Array[HBaseRawType])]) => {
        val config = wrappedConf.value
        /* "reduce task" <split #> <attempt # = spark task #> */
        val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false,
          context.partitionId(), context.attemptNumber())
        val hadoopContext = newTaskAttemptContext(config, attemptId)
        val format = new HFileOutputFormat2
        format match {
          case c: Configurable => c.setConf(config)
          case _ => ()
        }
        val committer = format.getOutputCommitter(hadoopContext).asInstanceOf[FileOutputCommitter]
        committer.setupTask(hadoopContext)

        val writer = format.getRecordWriter(hadoopContext).
          asInstanceOf[RecordWriter[ImmutableBytesWritable, KeyValue]]
        val bytesWritable = new ImmutableBytesWritable
        var recordsWritten = 0L
        var kv: (HBaseRawType, Array[HBaseRawType]) = null
        var prevK: HBaseRawType = null
        val columnFamilyNames =
          relation.htable.getTableDescriptor.getColumnFamilies.map(
            f => {
              f.getName
            })
        var isEmptyRow = true

        try {
          while (iter.hasNext) {
            kv = iter.next()

            if (prevK != null && Bytes.compareTo(kv._1, prevK) == 0) {
              // force flush because we cannot guarantee intra-row ordering
              logInfo(s"flushing HFile writer " + writer)
              // look at the type so we can print the name of the flushed file
              writer.write(null, null)
            }

            isEmptyRow = true
            for (i <- kv._2.indices) {
              if (kv._2(i).nonEmpty) {
                isEmptyRow = false
                val nkc = relation.nonKeyColumns(i)
                bytesWritable.set(kv._1)
                writer.write(bytesWritable, new KeyValue(kv._1, nkc.familyRaw,
                  nkc.qualifierRaw, kv._2(i)))
              }
            }

            if (isEmptyRow) {
              bytesWritable.set(kv._1)
              writer.write(bytesWritable,
                new KeyValue(
                  kv._1,
                  columnFamilyNames(0),
                  HConstants.EMPTY_BYTE_ARRAY,
                  HConstants.EMPTY_BYTE_ARRAY))
            }

            recordsWritten += 1

            prevK = kv._1
          }
        } finally {
          writer.close(hadoopContext)
        }

        committer.commitTask(hadoopContext)
        logInfo(s"commit HFiles in $tmpPath")

        val targetPath = committer.getCommittedTaskPath(hadoopContext)
        if (par) {
          val load = new LoadIncrementalHFiles(config)
          // there maybe no target path
          logInfo(s"written $recordsWritten records")
          if (recordsWritten > 0) {
            load.doBulkLoad(targetPath, relation.htable)
            relation.closeHTable()
          }
        }
        1
      }: Int

    @transient val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    @transient val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    @transient val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    logDebug(s"Starting doBulkLoad on table ${relation.htable.getName} ...")
    sqlContext.sparkContext.runJob(shuffled, writeShard)
    logDebug(s"finished BulkLoad : ${System.currentTimeMillis()}")
    jobCommitter.commitJob(jobTaskContext)
    if (!parallel) {
      val tablePath = new Path(tmpPath)
      val load = new LoadIncrementalHFiles(conf)
      load.doBulkLoad(tablePath, relation.htable)
    }
    relation.closeHTable()
    logDebug(s"finish BulkLoad on table ${relation.htable.getName}:" +
      s" ${System.currentTimeMillis()}")
    Seq.empty[Row]
  }

  override def output = Nil
}

