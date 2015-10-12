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

import java.io._

import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter.FilterBase
import org.apache.hadoop.hbase.util.{Bytes, Writables}
import org.apache.hadoop.hbase.{Cell, CellUtil, KeyValue}
import org.apache.hadoop.io.Writable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hbase.catalyst.expressions.HBaseMutableRows
import org.apache.spark.sql.hbase.catalyst.expressions.PartialPredicateOperations._
import org.apache.spark.sql.hbase.util.{BinaryBytesUtils, DataTypeUtils, HBaseKVHelper}
import org.apache.spark.sql.types.{AtomicType, DataType, StringType}

/**
 * The custom filter.  It will skip the scanner to the proper next position based on predicate.
 * This filter will only deal with the predicate containing key columns.
 *
 * The skip is multiple-dimensional on non-leading dimension keys in presence of the predicate's
 * range expressions; other types of expressions in the predicate will be eventually evaluated.
 *
 * The processing is stateful in that various info related to the previous processing is cached
 * and checked in the next invocations for maximum reuse.
 */
private[hbase] class HBaseCustomFilter extends FilterBase with Writable {

  /**
   *
   * @param dt the date type of this dimension
   * @param dimension the dimension index
   * @param parent the parent node reference
   * @param currentChildIndex the position of the current child
   * @param currentValue the value in the CPR
   * @param cpr the CPR of this dimension. If no critical points of this dimension present
   *            then this is going to be the "full" range
   * @param children the children nodes for a non-leaf node; otherwise null
   */
  private case class Node(dt: AtomicType = null, dimension: Int = -1, parent: Node = null,
                          var currentChildIndex: Int = -1, var currentValue: Any = null,
                          var cpr: CriticalPointRange[Any] = null,
                          var children: Seq[Node] = null) {

    // for full evaluation purpose
    lazy val boundRef = if (dimension == relation.dimSize - 1 && cpr != null && cpr.pred != null) {
      BindReferences.bindReference(cpr.pred, predReferences)
    } else {
      null
    }
  }

  private var relation: HBaseRelation = null
  private var predExpr: Expression = null
  private var predReferences: Seq[Attribute] = null
  private var predicateMap: Seq[(String, Int)] = null
  private val cellMap: scala.collection.mutable.Map[NonKeyColumn, Any] =
    scala.collection.mutable.Map[NonKeyColumn, Any]()

  // the root node: a placeholder for tree processing convenience
  private var root: Node = null

  // the current row key
  private var currentRowKey: HBaseRawType = null

  // the current row key values? may be useful ??
  private var currentValues: Seq[Any] = null

  // the next hint
  private var nextReturnCode: ReturnCode = null

  // the next key hint
  private var nextKeyValue: Cell = null

  // the next possible row key
  private var nextRowKey: HBaseRawType = null

  // the flag to determine whether to filter the remaining or not
  private var filterAllRemainingSetting: Boolean = false

  // flag of row change
  private var nextColFlag: Boolean = false

  // flag of filter row
  private var filterRowFlag: Boolean = false

  // the remaining predicate that can't be used for dimension range comparison
  private var remainingPredicate: Seq[Attribute] = null
  // cache of the bound ref for the node.children(childIndex)
  private var remainingPredicateBoundRef: Expression = null

  // the working row
  private var workingRow: HBaseMutableRows = null

  /**
   * constructor method
   * @param relation the relation
   * @param predExpr the predicate
   */
  def this(relation: HBaseRelation, predExpr: Expression) = {
    this()
    this.relation = relation
    this.predExpr = predExpr
  }


  /**
   * convert the relation / predicate to byte array, used by framework
   * @param dataOutput the output to write
   */
  override def write(dataOutput: DataOutput) = {
    val relationArray = HBaseSerializer.serialize(relation)
    Bytes.writeByteArray(dataOutput, relationArray)
    val predicateArray = HBaseSerializer.serialize(predExpr)
    Bytes.writeByteArray(dataOutput, predicateArray)
  }

  /**
   * convert byte array to relation / predicate, used by framework
   * @param dataInput the input to read
   */
  override def readFields(dataInput: DataInput) = {
    val relationArray = Bytes.readByteArray(dataInput)
    this.relation = HBaseSerializer.deserialize(relationArray).asInstanceOf[HBaseRelation]
    val predicateArray = Bytes.readByteArray(dataInput)
    this.predExpr = HBaseSerializer.deserialize(predicateArray).asInstanceOf[Expression]
    initialize()
  }

  override def toByteArray: Array[Byte] = {
    Writables.getBytes(this)
  }

  /**
   * initialize the variables based on the relation and predicate,
   * we also initialize the cpr cache for each dimension
   */
  private def initialize() = {
    predReferences = predExpr.references.toSeq
    workingRow = new HBaseMutableRows(predReferences.size)
    predicateMap = predReferences.map(a => a.name).zipWithIndex
    root = Node()

    filterAllRemainingSetting = false
    generateCPRs(root)
  }

  /**
   *
   * @param node the node to reset children on
   * @return
   */
  private def resetNode(node: Node) = {
    if (node != null && node.cpr != null) {
      node.currentValue = node.cpr.start.orNull
      if (node.currentValue != null && !node.cpr.startInclusive) {
        // if ths start is open-ended, try to add one
        addOne(node)
      }
    }
  }

  /**
   * recursively reset the index of the current child and the value in the child's CPR
   * @param node the start level, it will also reset its children
   */
  private def resetDescendants(node: Node): Unit = {
    if (node.children != null) {
      node.currentChildIndex = 0
      for (child <- node.children) {
        resetNode(child)
        resetDescendants(child)
      }
    }
  }

  /**
   * A quick top-down check whether the new row is in the current CPRs.
   * @param dimValues the current dimensional keys to check
   * @param dimLimit the lower bound of the dimensions to be checked with.
   *                 0 for the most significant dimension
   * @return whether the dimension keys are within the current ranges
   *         and for which dimension the keys start to be out of range or can't be checked at all
   */
  private def isInCurrentRanges(dimValues: Seq[Any], dimLimit: Int): (Boolean, Node) = {
    var node = root
    while (node.children != null && node.currentChildIndex != -1 &&
      node.currentValue != null &&
      node.children(node.currentChildIndex).dimension < dimLimit &&
      compareWithinRange(node.dt,
        dimValues(node.children(node.currentChildIndex).dimension),
        node.children(node.currentChildIndex).cpr) == 0) {
      node = node.children(node.currentChildIndex)
    }
    if (node.children == null) {
      (true, node)
    } else {
      (false, node)
    }
  }

  /**
   * Given the input kv (cell), filter it out, or keep it, or give the next hint
   * the decision is based on input predicate
   */
  override def filterKeyValue(kv: Cell): ReturnCode = {
    if (!nextColFlag) {
      // reset the index of each level
      currentRowKey = CellUtil.cloneRow(kv)
      nextColFlag = true
      val inputValues = relation.nativeKeyConvert(Some(currentRowKey))

      // node: the node that is
      // either the leaf that contains the current least significant key value; or
      // a nonleaf whose current child no longer contains the current key value
      val (inRange, node) = isInCurrentRanges(inputValues, relation.dimSize)

      if (node.dimension >= 0) {
        // for a non-root node, set its current value
        node.currentValue = inputValues(node.dimension)
      }
      if (inRange) {
        return ReturnCode.INCLUDE
      }
      remainingPredicate = null
      remainingPredicateBoundRef = null
      currentValues = inputValues
      resetDescendants(node)
      val result = findNextHint(node)
      nextReturnCode = result._1
      if (nextReturnCode == ReturnCode.SEEK_NEXT_USING_HINT) {
        nextRowKey = result._2
        nextKeyValue = new KeyValue(nextRowKey, CellUtil.cloneFamily(kv),
          HBaseCustomFilter.empty, HBaseCustomFilter.empty)
      } else if (nextReturnCode == ReturnCode.SKIP) {
        filterAllRemainingSetting = true
      }
    }

    nextReturnCode
  }

  /**
   * find the proper position of the value in the children using binary search
   *
   * @param node the node whose children to be searched for the value
   * @return (false, -1) if value is beyond the range of the largest child's CPR ;
   *         (true, childIndex) if the input is within a range and the index of the child;
   *         (false, nextCPRIndex) if the next position is not within a range
   *         but is smaller than the largest child's CPR
   */
  private def findPositionInRanges(node: Node): (Boolean, Int) = {
    require(node.children != null, "Internal logic error: children expected")
    val children = node.children
    if (children.isEmpty) {
      return (false, -1)
    }
    val dt: AtomicType = children.head.dt
    type t = dt.InternalType
    val value = currentValues(node.dimension + 1)

    var low: Int = node.currentChildIndex
    var high: Int = children.size - 1
    var middle: Int = 0
    // the flag to exit the while loop
    var found: Boolean = false
    while (high >= low && !found) {
      middle = (low + high) / 2
      // get the compare result
      val compare: Int = compareWithinRange(dt, value, children(middle).cpr)
      if (compare == 0) {
        // find the value in the range
        found = true
      } else if (compare == 1) {
        // increase the low value
        low = middle + 1
      } else if (compare == -1) {
        // decrease the high value
        high = middle - 1
      }
    }

    if (!found) {
      if (low > node.children.size - 1) {
        // no position found in the range
        (false, -1)
      } else {
        (false, low)
      }
    } else {
      (true, middle)
    }
  }

  /**
   * compare the input with a range [(previousRange.end, range.start)]
   * @param dt the data type
   * @param input the input value
   * @param cpr the critical point range to be tested
   * @return 0 within the range, -1 less than the range, 1 great than the range
   */
  private def compareWithinRange[T](dt: AtomicType, input: Any,
                                    cpr: CriticalPointRange[T]): Int = {
    val ordering = dt.ordering
    type t = dt.InternalType

    val start = cpr.start
    val startInclusive = cpr.startInclusive
    val end = cpr.end
    val endInclusive = cpr.endInclusive

    if (start.isDefined &&
      ((startInclusive && ordering.lt(input.asInstanceOf[t], start.get.asInstanceOf[t])) ||
        (!startInclusive && ordering.lteq(input.asInstanceOf[t], start.get.asInstanceOf[t])))) {
      -1
    } else if (end.isDefined &&
      ((endInclusive && ordering.gt(input.asInstanceOf[t], end.get.asInstanceOf[t])) ||
        (!endInclusive && ordering.gteq(input.asInstanceOf[t], end.get.asInstanceOf[t])))) {
      1
    } else {
      0
    }
  }

  /**
   * find the next hint based on the input byte array (row key as byte array)
   * @param node the node to start with (top-down traversal)
   * @return the tuple (ReturnCode, the improved row key)
   */
  private def findNextHint(node: Node): (ReturnCode, HBaseRawType) = {
    generateCPRs(node)
    // find the child for the value in the children
    val (found, childIndex) = findPositionInRanges(node)
    if (found) {
      if (node.currentChildIndex != childIndex) {
        require(childIndex >= node.currentChildIndex)
        for (i <- node.currentChildIndex until childIndex if node.dimension == -1) {
          // reset passed children to release the memory: only for the children
          // of the most significant dimension; higher dimensions' children are reusable
          if (i >= 0) {
            node.children(i).children = null
          }
        }
        node.currentChildIndex = childIndex
      }
      val child = node.children(childIndex)
      child.currentValue = currentValues(child.dimension)
      if (node.dimension == relation.dimSize - 2) {
        nextRowKey = buildRowKey()
        if (child.cpr != null && child.cpr.pred != null) {
          remainingPredicate = child.cpr.pred.references.toSeq
          remainingPredicateBoundRef = BindReferences.bindReference(child.cpr.pred,
            predReferences)
        }
        (ReturnCode.INCLUDE, nextRowKey)
      } else {
        findNextHint(node.children(node.currentChildIndex))
      }
    } else if (childIndex == -1) {
      // child goes out of ranges, bump current value if possible
      if (node.dimension == -1) {
        // a root
        (ReturnCode.SKIP, null)
      } else {
        increment(node)
      }
    } else {
      // cannot find a containing child but there is a larger child
      node.currentChildIndex = childIndex
      val child = node.children(childIndex)
      resetDescendants(child)
      if (child.cpr != null) {
        child.currentValue = child.cpr.start.orNull
        if (child.currentValue != null && !child.cpr.startInclusive) {
          // if ths start is open-ended, try to add one
          addOne(child)
        }
      }
      (ReturnCode.SEEK_NEXT_USING_HINT, buildRowKey())
    }
  }

  /**
   * upward increment a value in a dimension's CPR
   * @param node the node to start with
   * @return (return code, the row key after successful increment)
   */
  private def increment(node: Node): (ReturnCode, HBaseRawType) = {
    var currentNode: Node = node
    while (currentNode.parent != null) {
      if (addOne(currentNode)) {
        val cmp = compareWithinRange(currentNode.cpr.dt, currentNode.currentValue, currentNode.cpr)
        if (cmp == 0) {
          resetDescendants(currentNode)
          return (ReturnCode.SEEK_NEXT_USING_HINT, buildRowKey())
        } else {
          require(cmp > 0, "Internal logical error: unexpected ordering of row key")
          if (currentNode.parent.currentChildIndex < currentNode.parent.children.size - 1) {
            // get to the start of the next sibling CPR
            val childIndex = currentNode.parent.currentChildIndex + 1
            if (currentNode.dimension == 0) {
              // no look back: release the memory
              currentNode.children = null
            }
            resetDescendants(currentNode.parent)
            currentNode.parent.currentChildIndex = childIndex
            return (ReturnCode.SEEK_NEXT_USING_HINT, buildRowKey())
          } else {
            // beyond CPR's range of this dimension,
            // increment the next (more significant) dimension
            currentNode = currentNode.parent
          }
        }
      } else {
        return (ReturnCode.NEXT_ROW, null)
      }
    }
    (ReturnCode.SKIP, null)
  }

  /**
   *
   * @param node the node to add 1 to
   * @return whether the addition can be made within the value domain
   */
  private def addOne(node: Node): Boolean = {
    val dt = node.dt
    val value = node.currentValue
    var canAddOne: Boolean = true
    if (dt == StringType) {
      val newString = BinaryBytesUtils.addOneString(BinaryBytesUtils.create(dt).toBytes(value))
      val newValue = DataTypeUtils.bytesToData(newString, 0, newString.length, dt)
      node.currentValue = newValue
    } else {
      val newArray = BinaryBytesUtils.addOne(BinaryBytesUtils.create(dt).toBytes(value))
      if (newArray == null) {
        canAddOne = false
      } else {
        val newValue = DataTypeUtils.bytesToData(newArray, 0, newArray.length, dt)
        node.currentValue = newValue
      }
    }
    canAddOne
  }

  override def reset() = {
    nextColFlag = false
    filterRowFlag = false
  }

  /**
   * reset all the value in the row to be null
   * @param row the row to be reset
   */
  private def resetRow(row: HBaseMutableRows) = {
    // reset the row
    for (i <- 0 to row.numFields - 1) {
      row.update(i, null)
    }
  }

  /**
   * construct the row key based on the current currentValue of each dimension,
   * from dimension 0 to the dimIndex
   */
  private def buildRowKey(): HBaseRawType = {
    var list: List[(HBaseRawType, DataType)] = List[(HBaseRawType, DataType)]()
    var node = root
    while (node.currentChildIndex != -1 && node.children.nonEmpty &&
      node.children(node.currentChildIndex) != null &&
      node.children(node.currentChildIndex).currentValue != null) {
      val levelNode: Node = node.children(node.currentChildIndex)
      val dt = levelNode.dt
      val value = BinaryBytesUtils.create(dt).toBytes(levelNode.currentValue)
      list = list :+(value, dt)
      if (levelNode.dimension < relation.dimSize - 1) {
        generateCPRs(levelNode)
      }
      node = levelNode
    }
    HBaseKVHelper.encodingRawKeyColumns(list.toSeq)
  }

  /**
   * generate children for the current node based upon CPRs of all parent nodes if any,
   * and current CPR. For root, the current CPR is the original scan's
   */
  private def generateCPRs(node: Node): Unit = {
    require(node.dimension < relation.dimSize - 1,
      "Internal logical error: node of invalid dimension")
    if (node.children != null) {
      // if already computed, just return
      return
    }
    val dimIndex = node.dimension + 1
    val dt: AtomicType = relation.keyColumns(dimIndex).dataType.asInstanceOf[AtomicType]
    type t = dt.InternalType

    val keyDim = relation.partitionKeys(dimIndex)
    val predExpr = if (node.dimension == -1) {
      // this is the root: use the scan's predicate
      this.predExpr
    } else if (node.cpr == null) {
      null
    } else {
      node.cpr.pred
    }

    if (predExpr != null) {
      val criticalPoints: Seq[CriticalPoint[t]] = RangeCriticalPoint.collect(predExpr, keyDim)
      val predRefs = predExpr.references.toSeq
      val boundPred = BindReferences.bindReference(predExpr, predRefs)

      resetRow(workingRow)

      val qualifiedCPRanges = if (criticalPoints.nonEmpty) {
        // partial reduce
        val cpRanges: Seq[CriticalPointRange[t]] =
          RangeCriticalPoint.generateCriticalPointRange(criticalPoints, dimIndex, dt)

        // set values for all more significant dimensions
        var parent: Node = node
        while (parent.dimension >= 0) {
          val newKeyIndex = predRefs.indexWhere(_.exprId ==
            relation.partitionKeys(parent.dimension).exprId)
          if (newKeyIndex != -1) {
            workingRow.update(newKeyIndex, parent.currentValue)
          }
          parent = parent.parent
        }

        val keyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(dimIndex).exprId)
        cpRanges.filter(cpr => {
          workingRow.update(keyIndex, cpr)
          val prRes = boundPred.partialReduce(workingRow, predRefs)
          if (prRes._1 == null) cpr.pred = prRes._2
          prRes._1 == null || prRes._1.asInstanceOf[Boolean]
        })
      } else {
        Seq(new CriticalPointRange[t](None, false, None, false, dt, predExpr))
      }
      node.children = qualifiedCPRanges.map(range => {
        Node(dt, dimIndex, node, cpr = range)
      })
    } else {
      node.children = Seq(Node(dt, dimIndex, node,
        cpr = new CriticalPointRange[t](None, false, None, false, dt, null)))
    }
    resetDescendants(node)
  }

  /**
   * Do a full evaluation for the remaining predicate based on all the cell values.
   * @param kvs the list of cell
   */
  private def fullEvaluation(kvs: java.util.List[Cell]) = {
    resetRow(workingRow)
    cellMap.clear()
    for (i <- 0 to kvs.size() - 1) {
      val item = kvs.get(i)
      val data = CellUtil.cloneValue(item)
      if (data.nonEmpty) {
        val family = CellUtil.cloneFamily(item)
        val qualifier = CellUtil.cloneQualifier(item)
        val nkc = relation.nonKeyColumns.find(a =>
          Bytes.compareTo(a.familyRaw, family) == 0 &&
            Bytes.compareTo(a.qualifierRaw, qualifier) == 0).get
        val value = DataTypeUtils.bytesToData(
          data, 0, data.length, nkc.dataType, relation.bytesUtils)
        cellMap += (nkc -> value)
      }
    }
    for (item <- remainingPredicate) {
      relation.columnMap.get(item.name).get match {
        case nkc: NonKeyColumn =>
          val result = predicateMap.find(a => a._1 == nkc.sqlName).get
          val value = cellMap.get(nkc)
          if (value.isDefined) {
            workingRow.update(result._2, value.get)
          }
        case keyColumn: Int =>
          val keyIndex =
            predReferences.indexWhere(_.exprId == relation.partitionKeys(keyColumn).exprId)
          workingRow.update(keyIndex, currentValues(keyColumn))
      }
    }

    val result = remainingPredicateBoundRef.eval(workingRow)
    if (result != null && result.asInstanceOf[Boolean]) {
      filterRowFlag = false
    } else {
      filterRowFlag = true
    }
  }

  override def filterRowCells(kvs: java.util.List[Cell]) = {
    // In coprocessor, if the call to filterKeyValue returns INCLUDE on the very last record,
    // the scanner runs past the end and never call filterKeyValue() before reaching here, leading
    // to empty kvs and a subsequent NPE. This is observed with HBase 0.98.5.
    //
    // If a later HBase release has this addressed, this check will be made unnecessary
    // to save some CPU cycles
    if (kvs.isEmpty) filterRowFlag = true
    else if (remainingPredicate != null) fullEvaluation(kvs)
  }

  override def hasFilterRow: Boolean = {
    if (remainingPredicate != null) true else false
  }

  override def filterRow(): Boolean = {
    filterRowFlag
  }

  /**
   * decide whether to skip all the remaining or not
   * @return
   */
  override def filterAllRemaining() = {
    filterAllRemainingSetting
  }

  /**
   * determine where to skip to if filterKeyValue() returns SEEK_NEXT_USING_HINT
   * @param currentKV the current key value
   * @return the next possible key value
   */
  override def getNextCellHint(currentKV: Cell): Cell = {
    nextKeyValue
  }
}

object HBaseCustomFilter {
  val empty = Array[Byte]()

  def parseFrom(pbBytes: Array[Byte]): HBaseCustomFilter = {
    try {
      Writables.getWritable(pbBytes, new HBaseCustomFilter()).asInstanceOf[HBaseCustomFilter]
    } catch {
      case e: IOException => throw new DeserializationException(e)
    }
  }
}
