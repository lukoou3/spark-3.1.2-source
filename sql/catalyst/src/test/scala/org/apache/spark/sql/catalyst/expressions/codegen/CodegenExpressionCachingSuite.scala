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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A test suite that makes sure code generation handles expression internally states correctly.
 */
class CodegenExpressionCachingSuite extends SparkFunSuite {

  test("GenerateUnsafeProjection should initialize expressions") {
    // Use an Add to wrap two of them together in case we only initialize the top level expressions.
    val expr = And(NondeterministicExpression(), NondeterministicExpression())
    val instance = UnsafeProjection.create(Seq(expr))
    instance.initialize(0)
    assert(instance.apply(null).getBoolean(0) === false)
  }

  test("GenerateMutableProjection should initialize expressions") {
    val expr = And(NondeterministicExpression(), NondeterministicExpression())
    val instance = GenerateMutableProjection.generate(Seq(expr))
    instance.initialize(0)
    assert(instance.apply(null).getBoolean(0) === false)
  }

  test("GeneratePredicate should initialize expressions") {
    val expr = And(NondeterministicExpression(), NondeterministicExpression())
    val instance = GeneratePredicate.generate(expr)
    instance.initialize(0)
    assert(instance.eval(null) === false)
  }

  test("GenerateUnsafeProjection should not share expression instances") {
    val expr1 = MutableExpression()
    val instance1 = UnsafeProjection.create(Seq(expr1))
    assert(instance1.apply(null).getBoolean(0) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = UnsafeProjection.create(Seq(expr2))
    assert(instance1.apply(null).getBoolean(0) === false)
    assert(instance2.apply(null).getBoolean(0))
  }

  test("GenerateMutableProjection should not share expression instances") {
    val expr1 = MutableExpression()
    val instance1 = GenerateMutableProjection.generate(Seq(expr1))
    assert(instance1.apply(null).getBoolean(0) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = GenerateMutableProjection.generate(Seq(expr2))
    assert(instance1.apply(null).getBoolean(0) === false)
    assert(instance2.apply(null).getBoolean(0))
  }

  test("GeneratePredicate should not share expression instances") {
    val expr1 = MutableExpression()
    val instance1 = GeneratePredicate.generate(expr1)
    assert(instance1.eval(null) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = GeneratePredicate.generate(expr2)
    assert(instance1.eval(null) === false)
    assert(instance2.eval(null))
  }

  test("simple") {
    val expr = GreaterThan(BoundReference(0, IntegerType, true), BoundReference(1, IntegerType, true))
    val instance = GeneratePredicate.generate(expr)
    val row = new GenericInternalRow(new Array[Any](2))
    row.update(0, 1)
    row.update(1, 2)
    println(instance.eval(row))
    row.update(0, 2)
    row.update(1, 1)
    println(instance.eval(row))
  }

  test("genCode") {
    var ctx = new CodegenContext
    val b = BoundReference(0, IntegerType, true)
    val exprCode1: ExprCode = b.genCode(ctx)
    println(exprCode1)
    println("*"*10)
    println("*code:" + exprCode1.code)
    println("*isNull:" + exprCode1.isNull)
    println("*value:" + exprCode1.value)
    println("*"*60)
    ctx = new CodegenContext
    val expr = GreaterThan(BoundReference(0, IntegerType, true), BoundReference(1, IntegerType, true))
    val exprCode2: ExprCode = expr.genCode(ctx)
    println(exprCode2)
    println("*"*10)
    println("*code:" + exprCode2.code)
    println("*isNull:" + exprCode2.isNull)
    println("*value:" + exprCode2.value)
    println("*"*60)
    ctx = new CodegenContext
    val abs = Abs(BoundReference(0, IntegerType, true))
    val exprCode3: ExprCode = abs.genCode(ctx)
    println(exprCode3)
    println("*"*10)
    println("*code:" + exprCode3.code)
    println("*isNull:" + exprCode3.isNull)
    println("*value:" + exprCode3.value)

    println("*"*60)
    SQLConf.get.setConfString("spark.sql.codegen.comments", "true")
    ctx = new CodegenContext
    val abs2 = Abs(BoundReference(0, IntegerType, true))
    val exprCode4: ExprCode = abs2.genCode(ctx)
    println(exprCode4)
    println("*"*10)
    println("*code:" + exprCode4.code)
    println("*isNull:" + exprCode4.isNull)
    println("*value:" + exprCode4.value)
  }

  test("genCodeAddReferenceObj") {
    var ctx = new CodegenContext
    val b = Cast(BoundReference(2, DateType, true), StringType, Some("+08:00"))
    val exprCode1: ExprCode = b.genCode(ctx)
    println(exprCode1)
    println("*"*10)
    println("*code:" + exprCode1.code)
    println("*isNull:" + exprCode1.isNull)
    println("*value:" + exprCode1.value)
    println("*"*60)

  }

  test("simpleAddReferenceObj") {
    val expr = GreaterThan(
      BoundReference(0, StringType, true),
      Cast(BoundReference(1, DateType, true), StringType, Some("+08:00"))
    )
    val instance = GeneratePredicate.generate(expr)
    val row = new GenericInternalRow(new Array[Any](2))
    row.update(0, UTF8String.fromString("1970-01-02"))
    row.update(1, 1)
    println(instance.eval(row))
    row.update(0, UTF8String.fromString("1970-01-03"))
    row.update(1, 1)
    println(instance.eval(row))
  }
}

/**
 * An expression that's non-deterministic and doesn't support codegen.
 */
case class NondeterministicExpression()
  extends LeafExpression with Nondeterministic with CodegenFallback {
  override protected def initializeInternal(partitionIndex: Int): Unit = {}
  override protected def evalInternal(input: InternalRow): Any = false
  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType
}


/**
 * An expression with mutable state so we can change it freely in our test suite.
 */
case class MutableExpression() extends LeafExpression with CodegenFallback {
  var mutableState: Boolean = false
  override def eval(input: InternalRow): Any = mutableState

  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType
}
