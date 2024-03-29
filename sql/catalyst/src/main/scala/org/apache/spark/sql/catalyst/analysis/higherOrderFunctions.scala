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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType

/**
 * Resolve a higher order functions from the catalog. This is different from regular function
 * resolution because lambda functions can only be resolved after the function has been resolved;
 * so we need to resolve higher order function when all children are either resolved or a lambda
 * function.
 */
case class ResolveHigherOrderFunctions(catalog: SessionCatalog) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressions {
    case u @ UnresolvedFunction(fn, children, false, filter)
        if hasLambdaAndResolvedArguments(children) =>
      withPosition(u) {
        catalog.lookupFunction(fn, children) match {
          case func: HigherOrderFunction =>
            filter.foreach(_.failAnalysis("FILTER predicate specified, " +
              s"but ${func.prettyName} is not an aggregate function"))
            func
          case other => other.failAnalysis(
            "A lambda function should only be used in a higher order function. However, " +
              s"its class is ${other.getClass.getCanonicalName}, which is not a " +
              s"higher order function.")
        }
      }
  }

  /**
   * Check if the arguments of a function are either resolved or a lambda function.
   */
  private def hasLambdaAndResolvedArguments(expressions: Seq[Expression]): Boolean = {
    val (lambdas, others) = expressions.partition(_.isInstanceOf[LambdaFunction])
    lambdas.nonEmpty && others.forall(_.resolved)
  }
}

/**
 * Resolve the lambda variables exposed by a higher order functions.
 *
 * This rule works in two steps:
 * [1]. Bind the anonymous variables exposed by the higher order function to the lambda function's
 *      arguments; this creates named and typed lambda variables. The argument names are checked
 *      for duplicates and the number of arguments are checked during this step.
 * [2]. Resolve the used lambda variables used in the lambda function's function expression tree.
 *      Note that we allow the use of variables from outside the current lambda, this can either
 *      be a lambda function defined in an outer scope, or a attribute in produced by the plan's
 *      child. If names are duplicate, the name defined in the most inner scope is used.
 */
object ResolveLambdaVariables extends Rule[LogicalPlan] {

  type LambdaVariableMap = Map[String, NamedExpression]

  private def canonicalizer = {
    if (!conf.caseSensitiveAnalysis) {
      // scalastyle:off caselocale
      s: String => s.toLowerCase
      // scalastyle:on caselocale
    } else {
      s: String => s
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case q: LogicalPlan =>
        q.mapExpressions(resolve(_, Map.empty))
    }
  }

  /**
   * Create a bound lambda function by binding the arguments of a lambda function to the given
   * partial arguments (dataType and nullability only). If the expression happens to be an already
   * bound lambda function then we assume it has been bound to the correct arguments and do
   * nothing. This function will produce a lambda function with hidden arguments when it is passed
   * an arbitrary expression.
   */
  def createLambda(
      e: Expression,
      argInfo: Seq[(DataType, Boolean)]): LambdaFunction = e match {
    // arguments全部resolved直接返回，UnresolvedNamedLambdaVariable的resolved为false，NamedLambdaVariable的resolved为true
    case f: LambdaFunction if f.bound => f

    case LambdaFunction(function, names, _) =>
      if (names.size != argInfo.size) {
        e.failAnalysis(
          s"The number of lambda function arguments '${names.size}' does not " +
            "match the number of arguments expected by the higher order function " +
            s"'${argInfo.size}'.")
      }

      if (names.map(a => canonicalizer(a.name)).distinct.size < names.size) {
        e.failAnalysis(
          "Lambda function arguments should not have names that are semantically the same.")
      }

      val arguments = argInfo.zip(names).map {
        case ((dataType, nullable), ne) =>
          NamedLambdaVariable(ne.name, dataType, nullable)
      }
      LambdaFunction(function, arguments)

    case _ =>
      /**
       * 此表达式不使用lambda的任何参数（它是独立的）。
       * 创建LambdaFunction用默认的参数，因为higher order function会传入参数
       */
      // This expression does not consume any of the lambda's arguments (it is independent). We do
      // create a lambda function with default parameters because this is expected by the higher
      // order function. Note that we hide the lambda variables produced by this function in order
      // to prevent accidental naming collisions.
      val arguments = argInfo.zipWithIndex.map {
        case ((dataType, nullable), i) =>
          NamedLambdaVariable(s"col$i", dataType, nullable)
      }
      LambdaFunction(e, arguments, hidden = true)
  }

  /**
   * resolve lambda variables：UnresolvedNamedLambdaVariable => NamedLambdaVariable
   * Resolve lambda variables in the expression subtree, using the passed lambda variable registry.
   */
  def resolve(e: Expression, parentLambdaMap: LambdaVariableMap): Expression = e match {
    case _ if e.resolved => e

    case h: HigherOrderFunction if h.argumentsResolved && h.checkArgumentDataTypes().isSuccess =>
      h.bind(createLambda).mapChildren(resolve(_, parentLambdaMap))

    case l: LambdaFunction if !l.bound =>
      // 不要解析未绑定的lambda函数。如果我们看到这样一个lambda函数，这意味着要么高阶函数尚未解析，要么我们看到的是悬而未决的lambda函数。
      // Do not resolve an unbound lambda function. If we see such a lambda function this means
      // that either the higher order function has yet to be resolved, or that we are seeing
      // dangling lambda function.
      l

    case l: LambdaFunction if !l.hidden =>
      // LambdaFunction参数绑定的map
      val lambdaMap = l.arguments.map(v => canonicalizer(v.name) -> v).toMap
      // 函数中的变量绑定，下一步就是处理下面的UnresolvedNamedLambdaVariable
      l.mapChildren(resolve(_, parentLambdaMap ++ lambdaMap))

    case u @ UnresolvedNamedLambdaVariable(name +: nestedFields) =>
      parentLambdaMap.get(canonicalizer(name)) match {
        case Some(lambda) =>
          // 嵌套参数的处理，可以先忽略
          val r = nestedFields.foldLeft(lambda: Expression) { (expr, fieldName) =>
            ExtractValue(expr, Literal(fieldName), conf.resolver)
          }
          // 默认是lambda
          r
        case None =>
          UnresolvedAttribute(u.nameParts)
      }

    case _ =>
      e.mapChildren(resolve(_, parentLambdaMap))
  }
}
