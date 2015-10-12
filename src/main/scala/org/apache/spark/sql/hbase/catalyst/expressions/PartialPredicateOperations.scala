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

package org.apache.spark.sql.hbase.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hbase.types.RangeType._
import org.apache.spark.sql.hbase.types._
import org.apache.spark.sql.types._

object PartialPredicateOperations {

  // When the checkNullness argument of the partialReduce method is false, the partial
  // reduction is nullness-based, i.e., uninterested columns are assigned nulls,
  // which necessitates changes of the null handling from the normal evaluations
  // of predicate expressions. The IsNull/IsNotNull will return indefinite results.
  //
  // When the checkNullness argument of the partialReduce method is true, the "is null"
  // and "is not null" will return true or false in a definite manner; while other expressions
  // will evaluate to indefinite values.
  //
  // The whole mechanism is based upon the fact that any expression will evaluate to null
  // if any of its operands is null.
  //
  // There are 3 possible results: TRUE, FALSE, and MAYBE represented by a predicate
  // which will be used to further filter the results
  implicit class partialPredicateReducer(e: Expression) {
    /**
     * @param e the expression to be partially evaluated
     * @param schema the schema of 'e'
     * @return the original attribute for the bound reference
     */
    def unboundAttributeReference(e: Expression, schema: Seq[Attribute]): Expression = {
      e transform {
        case b: BoundReference => schema(b.ordinal)
      }
    }

    /**
     *
     * @param input rows to evaluate against
     * @param schema the schema of 'e'
     * @param checkNull the flag to check whether the partial evaluation is
     *                  for nullness checking purpose or not
     * @return
     */
    def partialReduce(input: InternalRow, schema: Seq[Attribute], checkNull: Boolean = false):
    (Any, Expression) = {
      e match {
        case And(left, right) =>
          val l = left.partialReduce(input, schema)
          if (l._1 == false) {
            (false, null)
          } else {
            val r = right.partialReduce(input, schema)
            if (r._1 == false) {
              (false, null)
            } else {
              (l._1, r._1) match {
                case (true, true) => (true, null)
                case (true, _) => (null, r._2)
                case (_, true) => (null, l._2)
                case (_, _) =>
                  if ((l._2 fastEquals left) && (r._2 fastEquals right)) {
                    (null, unboundAttributeReference(e, schema))
                  } else {
                    (null, And(l._2, r._2))
                  }
                case _ => sys.error("unexpected child type(s) in partial reduction")
              }
            }
          }
        case Or(left, right) =>
          val l = left.partialReduce(input, schema)
          if (l._1 == true) {
            (true, null)
          } else {
            val r = right.partialReduce(input, schema)
            if (r._1 == true) {
              (true, null)
            } else {
              (l._1, r._1) match {
                case (false, false) => (false, null)
                case (false, _) => (null, r._2)
                case (_, false) => (null, l._2)
                case (_, _) =>
                  if ((l._2 fastEquals left) && (r._2 fastEquals right)) {
                    (null, unboundAttributeReference(e, schema))
                  } else {
                    (null, Or(l._2, r._2))
                  }
                case _ => sys.error("unexpected child type(s) in partial reduction")
              }
            }
          }
        case Not(child) =>
          child.partialReduce(input, schema) match {
            case (b: Boolean, null) => (!b, null)
            case (null, ec: Expression) => if (ec fastEquals child) {
              (null, unboundAttributeReference(e, schema))
            } else {
              (null, Not(ec))
            }
          }
        case In(value, list) =>
          val (evaluatedValue, expr) = value.partialReduce(input, schema)
          if (evaluatedValue == null) {
            val evaluatedList = list.map(e => e.partialReduce(input, schema) match {
              case (null, e: Expression) => e
              case (d, _) => Literal.create(d, e.dataType)
            })
            (null, In(expr, evaluatedList))
          } else {
            val evaluatedList: Seq[(Any, Expression)] = list.map(_.partialReduce(input, schema))
            var foundInList = false
            var newList = List[Expression]()
            for (item <- evaluatedList if !foundInList) {
              if (item._1 == null) {
                newList = newList :+ item._2
              } else {
                val cmp = prc2(input, value.dataType, item._2.dataType, evaluatedValue, item._1)
                if (cmp.isDefined && cmp.get == 0) {
                  foundInList = true
                } else if (cmp.isEmpty || (cmp.isDefined && (cmp.get == 1 || cmp.get == -1))) {
                  newList = newList :+ item._2
                }
              }
            }
            if (foundInList) {
              (true, null)
            } else if (newList.isEmpty) {
              (false, null)
            } else {
              (null, In(expr, newList))
            }
          }
        case InSet(value, hset) =>
          val evaluatedValue = value.partialReduce(input, schema)
          if (evaluatedValue._1 == null) {
            (null, InSet(evaluatedValue._2, hset))
          } else {
            var foundInSet = false
            var newHset = Set[Any]()
            for (item <- hset if !foundInSet) {
              val cmp = prc2(input, value.dataType, value.dataType, evaluatedValue._1, item)
              if (cmp.isDefined && cmp.get == 0) {
                foundInSet = true
              } else if (cmp.isEmpty || (cmp.isDefined && (cmp.get == 1 || cmp.get == -1))) {
                newHset = newHset + item
              }
            }
            if (foundInSet) {
              (true, null)
            } else if (newHset.isEmpty) {
              (false, null)
            } else {
              (null, InSet(evaluatedValue._2, newHset))
            }
          }
        case b: BoundReference =>
          val res = input match {
            case r: HBaseMutableRows => r.genericGet(b.ordinal)
            case r: GenericInternalRow => r.values(b.ordinal)
            case _ => input.copy().asInstanceOf[GenericInternalRow].values(b.ordinal)
          }
          (res, schema(b.ordinal))
        case l: LeafExpression =>
          val res = l.eval(input)
          (res, l)
        case n: NamedExpression =>
          val res = n.eval(input)
          (res, n)
        case IsNull(child) => if (checkNull) {
          if (child == null) {
            (true, null)
          } else {
            (false, null)
          }
        } else {
          (null, unboundAttributeReference(e, schema))
        }
        case IsNotNull(child) => if (checkNull) {
          if (child == null) {
            (false, null)
          } else {
            (true, null)
          }
        } else {
          (null, unboundAttributeReference(e, schema))
        }
        // TODO: CAST/Arithmetic could be treated more nicely
        case Cast(_, _) => (null, unboundAttributeReference(e, schema))
        // case BinaryArithmetic => null
        case UnaryMinus(_) => (null, unboundAttributeReference(e, schema))
        case EqualTo(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, EqualTo(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, EqualTo(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, EqualTo(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined && cmp.get != 1 && cmp.get != -1) {
              (cmp.get == 0, null)
            } else {
              (null, EqualTo(evalL._2, evalR._2))
            }
          }
        case LessThan(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, LessThan(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, LessThan(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, LessThan(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined && cmp.get != -1) {
              (cmp.get == -2, null)
            } else {
              (null, LessThan(evalL._2, evalR._2))
            }
          }
        case LessThanOrEqual(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, LessThanOrEqual(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, LessThanOrEqual(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, LessThanOrEqual(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined) {
              if (cmp.get == 1) {
                (null, EqualTo(evalL._2, evalR._2))
              } else {
                (cmp.get <= 0, null)
              }
            } else {
              (null, LessThanOrEqual(evalL._2, evalR._2))
            }
          }
        case GreaterThan(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, GreaterThan(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, GreaterThan(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, GreaterThan(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined && cmp.get != 1) {
              (cmp.get == 2, null)
            } else {
              (null, GreaterThan(evalL._2, evalR._2))
            }
          }
        case GreaterThanOrEqual(left, right) =>
          val evalL = left.partialReduce(input, schema)
          val evalR = right.partialReduce(input, schema)
          if (evalL._1 == null && evalR._1 == null) {
            (null, GreaterThanOrEqual(evalL._2, evalR._2))
          } else if (evalL._1 == null) {
            (null, GreaterThanOrEqual(evalL._2, right))
          } else if (evalR._1 == null) {
            (null, GreaterThanOrEqual(left, evalR._2))
          } else {
            val cmp = prc2(input, left.dataType, right.dataType, evalL._1, evalR._1)
            if (cmp.isDefined) {
              if (cmp.get == -1) {
                (null, EqualTo(evalL._2, evalR._2))
              } else {
                (cmp.get >= 0, null)
              }
            } else {
              (null, GreaterThanOrEqual(evalL._2, evalR._2))
            }
          }
        case If(predicate, trueE, falseE) =>
          val (v, _) = predicate.partialReduce(input, schema)
          if (v == null) {
            (null, unboundAttributeReference(e, schema))
          } else if (v.asInstanceOf[Boolean]) {
            trueE.partialReduce(input, schema)
          } else {
            falseE.partialReduce(input, schema)
          }
        case _ => (null, unboundAttributeReference(e, schema))
      }
    }

    @inline
    protected def prc2(
                        i: InternalRow,
                        dataType1: DataType,
                        dataType2: DataType,
                        eval1: Any,
                        eval2: Any): Option[Int] = {
      if (dataType1 != dataType2) {
        throw new TreeNodeException(e, s"Types do not match $dataType1 != $dataType2")
      }

      dataType1 match {
        case nativeType: AtomicType =>
          val pdt: RangeType[nativeType.InternalType] = {
            nativeType.toRangeType[nativeType.InternalType]
          }
          pdt.partialOrdering.tryCompare(
            pdt.toPartiallyOrderingDataType(eval1, nativeType),
            pdt.toPartiallyOrderingDataType(eval2, nativeType))
        case other => sys.error(s"Type $other does not support partially ordered operations")
      }
    }
  }

}
