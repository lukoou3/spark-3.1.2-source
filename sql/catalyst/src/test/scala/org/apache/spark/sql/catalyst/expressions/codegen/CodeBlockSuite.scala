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
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.IntegerType

class CodeBlockSuite extends SparkFunSuite {

  // block插值字符串和表达值输入
  test("Block interpolates string and ExprValue inputs") {
    // 创建一个isNull的局部变量, name为expr1_isNull
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    // 字符串字面量
    val stringLiteral = "false"
    // code插值
    val code = code"boolean $isNull = $stringLiteral;"
    // 生成的代码
    assert(code.toString == "boolean expr1_isNull = false;")
  }

  // 直接的字面量被折叠成字符串代码部分，而不是块输入
  test("Literals are folded into string code parts instead of block inputs") {
    val value = JavaCode.variable("expr1", IntegerType)
    val intLiteral = 1
    val code = code"int $value = $intLiteral;"
    assert(code.asInstanceOf[CodeBlock].blockInputs === Seq(value))
  }

  // 应将代码部分视为转义，但不应将字符串输入视为转义
  test("Code parts should be treated for escapes, but string inputs shouldn't be") {
    val strlit = raw"\\"
    val code = code"""String s = "foo\\bar" + "$strlit";"""

    val builtin = s"""String s = "foo\\bar" + "$strlit";"""

    val expected = raw"""String s = "foo\bar" + "\\";"""

    assert(builtin == expected)
    assert(code.asInstanceOf[CodeBlock].toString == expected)
  }

  // 测试Block.stripMargin
  test("Block.stripMargin") {
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val value = JavaCode.variable("expr1", IntegerType)
    val code1 =
      code"""
           |boolean $isNull = false;
           |int $value = ${JavaCode.defaultLiteral(IntegerType)};""".stripMargin
    val expected =
      s"""
        |boolean expr1_isNull = false;
        |int expr1 = ${JavaCode.defaultLiteral(IntegerType)};""".stripMargin.trim
    assert(code1.toString == expected)

    val code2 =
      code"""
           >boolean $isNull = false;
           >int $value = ${JavaCode.defaultLiteral(IntegerType)};""".stripMargin('>')
    assert(code2.toString == expected)
  }

  // Block可以捕获输入expr values
  test("Block can capture input expr values") {
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val value = JavaCode.variable("expr1", IntegerType)
    val code =
      code"""
           |boolean $isNull = false;
           |int $value = -1;
          """.stripMargin
    val exprValues = code.asInstanceOf[CodeBlock].blockInputs.collect {
      case e: ExprValue => e
    }.toSet
    assert(exprValues.size == 2)
    assert(exprValues === Set(value, isNull))
  }

  // 合并blocks
  test("concatenate blocks") {
    val isNull1 = JavaCode.isNullVariable("expr1_isNull")
    val value1 = JavaCode.variable("expr1", IntegerType)
    val isNull2 = JavaCode.isNullVariable("expr2_isNull")
    val value2 = JavaCode.variable("expr2", IntegerType)
    val literal = JavaCode.literal("100", IntegerType)

    val code =
      code"""
           |boolean $isNull1 = false;
           |int $value1 = -1;""".stripMargin +
      code"""
           |boolean $isNull2 = true;
           |int $value2 = $literal;""".stripMargin

    val expected =
      """
       |boolean expr1_isNull = false;
       |int expr1 = -1;
       |boolean expr2_isNull = true;
       |int expr2 = 100;""".stripMargin.trim

    assert(code.toString == expected)

    val exprValues = code.children.flatMap(_.asInstanceOf[CodeBlock].blockInputs.collect {
      case e: ExprValue => e
    }).toSet
    assert(exprValues.size == 5)
    //  LiteralValue也在blockInputs中, 直接在code中的插入的字面量非Litera类, 才会被处理成code字符串
    // 看了code的实现, 发现只有ExprValue和CodeBlock会放到输入里, Inline和其他的java基本类型(包含str)会放入code
    // code中不允许插入其他的类型, 会直接抛出异常
    assert(exprValues === Set(isNull1, value1, isNull2, value2, literal))
  }

  // 在代码块中插入未接受的对象时抛出异常, 接受JavaCode的子类和java基本类型(包含str)
  // ExprValue和CodeBlock会放到输入里, Inline和其他的java基本类型(包含str)会放入code
  test("Throws exception when interpolating unexcepted object in code block") {
    val obj = Tuple2(1, 1)
    val e = intercept[IllegalArgumentException] {
      code"$obj"
    }
    assert(e.getMessage().contains(s"Can not interpolate ${obj.getClass.getName}"))
  }

  // 转换block中的expr
  test("transform expr in code block") {
    // SimpleExprValue
    val expr = JavaCode.expression("1 + 1", IntegerType)
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val exprInFunc = JavaCode.variable("expr1", IntegerType)

    val code =
      code"""
           |callFunc(int $expr) {
           |  boolean $isNull = false;
           |  int $exprInFunc = $expr + 1;
           |}""".stripMargin

    val aliasedParam = JavaCode.variable("aliased", expr.javaType)

    // We want to replace all occurrences of `expr` with the variable `aliasedParam`.
    val aliasedCode = code.transformExprValues {
      case SimpleExprValue("1 + 1", java.lang.Integer.TYPE) => aliasedParam
    }
    val expected =
      code"""
           |callFunc(int $aliasedParam) {
           |  boolean $isNull = false;
           |  int $exprInFunc = $aliasedParam + 1;
           |}""".stripMargin
    assert(aliasedCode.toString == expected.toString)
  }

  test ("transform expr in nested blocks") {
    val expr = JavaCode.expression("1 + 1", IntegerType)
    val isNull = JavaCode.isNullVariable("expr1_isNull")
    val exprInFunc = JavaCode.variable("expr1", IntegerType)

    val funcs = Seq("callFunc1", "callFunc2", "callFunc3")
    val subBlocks = funcs.map { funcName =>
      code"""
           |$funcName(int $expr) {
           |  boolean $isNull = false;
           |  int $exprInFunc = $expr + 1;
           |}""".stripMargin
    }

    val aliasedParam = JavaCode.variable("aliased", expr.javaType)

    val block = code"${subBlocks(0)}\n${subBlocks(1)}\n${subBlocks(2)}"
    val transformedBlock = block.transform {
      case b: Block => b.transformExprValues {
        case SimpleExprValue("1 + 1", java.lang.Integer.TYPE) => aliasedParam
      }
    }.asInstanceOf[CodeBlock]

    val expected1 =
      code"""
        |callFunc1(int aliased) {
        |  boolean expr1_isNull = false;
        |  int expr1 = aliased + 1;
        |}""".stripMargin

    val expected2 =
      code"""
        |callFunc2(int aliased) {
        |  boolean expr1_isNull = false;
        |  int expr1 = aliased + 1;
        |}""".stripMargin

    val expected3 =
      code"""
        |callFunc3(int aliased) {
        |  boolean expr1_isNull = false;
        |  int expr1 = aliased + 1;
        |}""".stripMargin

    val exprValues = transformedBlock.children.flatMap { block =>
      block.asInstanceOf[CodeBlock].blockInputs.collect {
        case e: ExprValue => e
      }
    }.toSet

    assert(transformedBlock.children(0).toString == expected1.toString)
    assert(transformedBlock.children(1).toString == expected2.toString)
    assert(transformedBlock.children(2).toString == expected3.toString)
    assert(transformedBlock.toString == (expected1 + expected2 + expected3).toString)
    assert(exprValues === Set(isNull, exprInFunc, aliasedParam))
  }
}
