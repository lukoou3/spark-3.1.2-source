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

package org.apache.spark.sql.execution.aggregate

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/**
 * The base class of [[SortBasedAggregationIterator]] and [[TungstenAggregationIterator]].
 * It mainly contains two parts:
 * 1. It initializes aggregate functions.
 * 2. It creates two functions, `processRow` and `generateOutput` based on [[AggregateMode]] of
 *    its aggregate functions. `processRow` is the function to handle an input. `generateOutput`
 *    is used to generate result.
 */
abstract class AggregationIterator(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    inputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection)
  extends Iterator[UnsafeRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Initializing functions.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * The following combinations of AggregationMode are supported:
   * - Partial
   * - PartialMerge (for single distinct)
   * - Partial and PartialMerge (for single distinct)
   * - Final
   * - Complete (for SortAggregate with functions that does not support Partial)
   * - Final and Complete (currently not used)
   *
   * TODO: AggregateMode should have only two modes: Update and Merge, AggregateExpression
   * could have a flag to tell it's final or not.
   */
  {
    val modes = aggregateExpressions.map(_.mode).distinct.toSet
    require(modes.size <= 2,
      s"$aggregateExpressions are not supported because they have more than 2 distinct modes.")
    require(modes.subsetOf(Set(Partial, PartialMerge)) || modes.subsetOf(Set(Final, Complete)),
      s"$aggregateExpressions can't have Partial/PartialMerge and Final/Complete in the same time.")
  }

  // 初始化所有的AggregateFunctions绑定references，设置inputBufferOffset and mutableBufferOffset.
  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
  protected def initializeAggregateFunctions(
      expressions: Seq[AggregateExpression],
      startingInputBufferOffset: Int): Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = startingInputBufferOffset
    // 聚合函数数组
    val functions = new Array[AggregateFunction](expressions.length)
    var i = 0
    val inputAttributeSeq: AttributeSeq = inputAttributes
    for (expression <- expressions) {
      val func = expression.aggregateFunction
      val funcWithBoundReferences: AggregateFunction = expression.mode match {
        case Partial | Complete if func.isInstanceOf[ImperativeAggregate] =>
          // 函数不是基于表达式的聚合函数（不支持代码生成）并且是Partial或者Complete模式。
          // 调用函数的children的eval更新aggregate function。
          // 这些eval调用需要BoundReferences才能工作。
          // We need to create BoundReferences if the function is not an
          // expression-based aggregate function (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, inputAttributeSeq)
        case _ =>
          // 其他情况仅仅需要设置inputBufferOffset
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          val updatedFunc = func match {
            case function: ImperativeAggregate =>
              function.withNewInputAggBufferOffset(inputBufferOffset)
            case function => function
          }
          inputBufferOffset += func.aggBufferSchema.length
          updatedFunc
      }
      val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
        case function: ImperativeAggregate =>
          // Set mutableBufferOffset for this function. It is important that setting
          // mutableBufferOffset happens after all potential bindReference operations
          // because bindReference will create a new instance of the function.
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  protected val aggregateFunctions: Array[AggregateFunction] =
    initializeAggregateFunctions(aggregateExpressions, initialInputBufferOffset)

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions.
  // ImperativeAggregateFunctionPositions will be [1, 2].
  protected[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < aggregateFunctions.length) {
      aggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // The projection used to initialize buffer values for all expression-based aggregates.
  protected[this] val expressionAggInitialProjection = {
    val initExpressions = aggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      case i: ImperativeAggregate => Seq.fill(i.aggBufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)
  }

  // All imperative AggregateFunctions.
  protected[this] val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
    allImperativeAggregateFunctionPositions
      .map(aggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])

  // Initializing functions used to process a row.
  protected def generateProcessRow(
      expressions: Seq[AggregateExpression],
      functions: Seq[AggregateFunction],
      inputAttributes: Seq[Attribute]): (InternalRow, InternalRow) => Unit = {
    val joinedRow = new JoinedRow
    if (expressions.nonEmpty) {
      // mergeExpressions聚合函数更新，这里使用的是flatMap，这样Average等就能够和其它sum等函数展平在一个row里处理了
      val mergeExpressions =
        functions.zip(expressions.map(ae => (ae.mode, ae.isDistinct, ae.filter))).flatMap {
          case (ae: DeclarativeAggregate, (mode, isDistinct, filter)) =>
            mode match {
              case Partial | Complete =>
                if (filter.isDefined) {
                  ae.updateExpressions.zip(ae.aggBufferAttributes).map {
                    case (updateExpr, attr) => If(filter.get, updateExpr, attr)
                  }
                } else {
                  ae.updateExpressions
                }
              case PartialMerge | Final => ae.mergeExpressions
            }
          case (agg: AggregateFunction, _) => Seq.fill(agg.aggBufferAttributes.length)(NoOp) // ImperativeAggregate聚合函数站位
        }
      // Initialize predicates for aggregate functions if necessary
      val predicateOptions = expressions.map {
        case AggregateExpression(_, mode, _, Some(filter), _) =>
          mode match {
            case Partial | Complete =>
              val predicate = Predicate.create(filter, inputAttributes)
              predicate.initialize(partIndex)
              Some(predicate)
            case _ => None
          }
        case _ => None
      }
      // 更新ImperativeAggregate聚合函数
      val updateFunctions = functions.zipWithIndex.collect {
        case (ae: ImperativeAggregate, i) =>
          expressions(i).mode match {
            case Partial | Complete =>
              if (predicateOptions(i).isDefined) {
                (buffer: InternalRow, row: InternalRow) =>
                  if (predicateOptions(i).get.eval(row)) { ae.update(buffer, row) }
              } else {
                (buffer: InternalRow, row: InternalRow) => ae.update(buffer, row)
              }
            case PartialMerge | Final =>
              (buffer: InternalRow, row: InternalRow) => ae.merge(buffer, row)
          }
      }.toArray
      // This projection is used to merge buffer values for all expression-based aggregates.
      val aggregationBufferSchema = functions.flatMap(_.aggBufferAttributes)
      // 更新expression-based aggregates的逻辑
      val updateProjection =
        newMutableProjection(mergeExpressions, aggregationBufferSchema ++ inputAttributes)

      (currentBuffer: InternalRow, row: InternalRow) => {
        // map阶段：就是把flatMap后的AggregateFunction处理结果update到mutableRow
        // reduce阶段：也是是把flatMap后的AggregateFunction处理结果update到mutableRow，就是输入Schema不同
        // Process all expression-based aggregate functions.
        updateProjection.target(currentBuffer)(joinedRow(currentBuffer, row))
        // Process all imperative aggregate functions.
        var i = 0
        while (i < updateFunctions.length) {
          updateFunctions(i)(currentBuffer, row)
          i += 1
        }
      }
    } else {
      // Grouping only.
      (currentBuffer: InternalRow, row: InternalRow) => {}
    }
  }

  protected val processRow: (InternalRow, InternalRow) => Unit =
    generateProcessRow(aggregateExpressions, aggregateFunctions, inputAttributes)

  protected val groupingProjection: UnsafeProjection =
    UnsafeProjection.create(groupingExpressions, inputAttributes)
  protected val groupingAttributes = groupingExpressions.map(_.toAttribute)

  // 输出结果row
  // Initializing the function used to generate the output row.
  protected def generateResultProjection(): (UnsafeRow, InternalRow) => UnsafeRow = {
    val joinedRow = new JoinedRow
    val modes = aggregateExpressions.map(_.mode).distinct
    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    // reduce阶段的输出
    if (modes.contains(Final) || modes.contains(Complete)) {
      val evalExpressions = aggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression
        case agg: AggregateFunction => NoOp
      }
      val aggregateResult = new SpecificInternalRow(aggregateAttributes.map(_.dataType))
      val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)
      expressionAggEvalProjection.target(aggregateResult) // expressionAggEvalProjection更新aggregateResult

      val resultProjection =
        UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateAttributes)
      resultProjection.initialize(partIndex)

      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        // expressionAggEvalProjection也是更新aggregateResult
        // Generate results for all expression-based aggregate functions.
        expressionAggEvalProjection(currentBuffer)
        // Generate results for all imperative aggregate functions.
        var i = 0
        while (i < allImperativeAggregateFunctions.length) {
          aggregateResult.update(
            allImperativeAggregateFunctionPositions(i),
            allImperativeAggregateFunctions(i).eval(currentBuffer))
          i += 1
        }
        // 就是把key和聚合值合在一起输出
        resultProjection(joinedRow(currentGroupingKey, aggregateResult))
      }
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // map阶段的输出
      val resultProjection = UnsafeProjection.create(
        groupingAttributes ++ bufferAttributes,
        groupingAttributes ++ bufferAttributes)
      resultProjection.initialize(partIndex)

      // TypedImperativeAggregate stores generic object in aggregation buffer, and requires
      // calling serialization before shuffling. See [[TypedImperativeAggregate]] for more info.
      val typedImperativeAggregates: Array[TypedImperativeAggregate[_]] = {
        aggregateFunctions.collect {
          case (ag: TypedImperativeAggregate[_]) => ag
        }
      }

      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        // Serializes the generic object stored in aggregation buffer
        var i = 0
        while (i < typedImperativeAggregates.length) {
          typedImperativeAggregates(i).serializeAggregateBufferInPlace(currentBuffer)
          i += 1
        }
        resultProjection(joinedRow(currentGroupingKey, currentBuffer))
      }
    } else {
      // Grouping-only: we only output values based on grouping expressions.
      val resultProjection = UnsafeProjection.create(resultExpressions, groupingAttributes)
      resultProjection.initialize(partIndex)
      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        resultProjection(currentGroupingKey)
      }
    }
  }

  protected val generateOutput: (UnsafeRow, InternalRow) => UnsafeRow =
    generateResultProjection()

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(buffer: InternalRow): Unit = {
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    var i = 0
    while (i < allImperativeAggregateFunctions.length) {
      allImperativeAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }
}
