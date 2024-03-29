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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{IntervalUtils, TypeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the negated value of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       -1
  """,
  since = "1.0.0")
case class UnaryMinus(
    child: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  def this(child: Expression) = this(child, SQLConf.get.ansiEnabled)

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case _: DecimalType => defineCodeGen(ctx, ev, c => s"$c.unary_$$minus()")
    case ByteType | ShortType if failOnError =>
      nullSafeCodeGen(ctx, ev, eval => {
        val javaBoxedType = CodeGenerator.boxedType(dataType)
        val javaType = CodeGenerator.javaType(dataType)
        val originValue = ctx.freshName("origin")
        s"""
           |$javaType $originValue = ($javaType)($eval);
           |if ($originValue == $javaBoxedType.MIN_VALUE) {
           |  throw new ArithmeticException("- " + $originValue + " caused overflow.");
           |}
           |${ev.value} = ($javaType)(-($originValue));
           """.stripMargin
      })
    case IntegerType | LongType if failOnError =>
      nullSafeCodeGen(ctx, ev, eval => {
        val mathClass = classOf[Math].getName
        s"${ev.value} = $mathClass.negateExact($eval);"
      })
    case dt: NumericType => nullSafeCodeGen(ctx, ev, eval => {
      val originValue = ctx.freshName("origin")
      // codegen would fail to compile if we just write (-($c))
      // for example, we could not write --9223372036854775808L in code
      s"""
        ${CodeGenerator.javaType(dt)} $originValue = (${CodeGenerator.javaType(dt)})($eval);
        ${ev.value} = (${CodeGenerator.javaType(dt)})(-($originValue));
      """})
    case _: CalendarIntervalType =>
      val iu = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      val method = if (failOnError) "negateExact" else "negate"
      defineCodeGen(ctx, ev, c => s"$iu.$method($c)")
  }

  protected override def nullSafeEval(input: Any): Any = dataType match {
    case CalendarIntervalType if failOnError =>
      IntervalUtils.negateExact(input.asInstanceOf[CalendarInterval])
    case CalendarIntervalType => IntervalUtils.negate(input.asInstanceOf[CalendarInterval])
    case _ => numeric.negate(input)
  }

  override def sql: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("-") match {
      case "-" => s"(- ${child.sql})"
      case funcName => s"$funcName(${child.sql})"
    }
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the value of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       1
  """,
  since = "1.5.0")
case class UnaryPositive(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  protected override def nullSafeEval(input: Any): Any = input

  override def sql: String = s"(+ ${child.sql})"
}

/**
 * 继承的接口:
 *    一元表达式:一个参数
 *    ExpectsInputTypes: 具有预期输入类型的表达式
 *    NullIntolerant:
 * A function that get the absolute value of the numeric value.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the absolute value of the numeric value.",
  examples = """
    Examples:
      > SELECT _FUNC_(-1);
       1
  """,
  since = "1.2.0")
case class Abs(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  // dataType和child一样
  override def dataType: DataType = child.dataType

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    // java.math.BigDecimal
    case _: DecimalType =>
      defineCodeGen(ctx, ev, c => s"$c.abs()")
    // 数值类型, 调用java.lang.Math.abs(变量名), 强转为输入(也是输出)的类型
    // CodeGenerator.javaType(dt)返回spark sql DataType对应的java类型, 可以看到DateType,TimestampType底层是用int,long表示的
    case dt: NumericType =>
      defineCodeGen(ctx, ev, c => s"(${CodeGenerator.javaType(dt)})(java.lang.Math.abs($c))")
  }

  protected override def nullSafeEval(input: Any): Any = numeric.abs(input)
}

abstract class BinaryArithmetic extends BinaryOperator with NullIntolerant {

  // 代码生成函数使用
  protected val failOnError: Boolean

  override def dataType: DataType = left.dataType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  // 代码生成函数使用
  /** Name of the function for this expression on a [[Decimal]] type. */
  def decimalMethod: String =
    sys.error("BinaryArithmetics must override either decimalMethod or genCode")

  // 代码生成函数使用
  /** Name of the function for this expression on a [[CalendarInterval]] type. */
  def calendarIntervalMethod: String =
    sys.error("BinaryArithmetics must override either calendarIntervalMethod or genCode")

  // Name of the function for the exact version of this expression in [[Math]].
  // If the option "spark.sql.ansi.enabled" is enabled and there is corresponding
  // function in [[Math]], the exact function will be called instead of evaluation with [[symbol]].
  def exactMathMethod: Option[String] = None

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
  dataType match {
    case _: DecimalType =>
      // Overflow is handled in the CheckOverflow operator
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$decimalMethod($eval2)")
    case CalendarIntervalType =>
      val iu = IntervalUtils.getClass.getCanonicalName.stripSuffix("$")
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$iu.$calendarIntervalMethod($eval1, $eval2)")
    // byte and short are casted into int when add, minus, times or divide
    case ByteType | ShortType =>
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        val tmpResult = ctx.freshName("tmpResult")
        val overflowCheck = if (failOnError) {
          val javaType = CodeGenerator.boxedType(dataType)
          s"""
             |if ($tmpResult < $javaType.MIN_VALUE || $tmpResult > $javaType.MAX_VALUE) {
             |  throw new ArithmeticException($eval1 + " $symbol " + $eval2 + " caused overflow.");
             |}
           """.stripMargin
        } else {
          ""
        }
        s"""
           |${CodeGenerator.JAVA_INT} $tmpResult = $eval1 $symbol $eval2;
           |$overflowCheck
           |${ev.value} = (${CodeGenerator.javaType(dataType)})($tmpResult);
         """.stripMargin
      })
    case IntegerType | LongType =>
      if(true){
        //throw new Exception("test")
      }
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        val operation = if (failOnError && exactMathMethod.isDefined) {
          val mathClass = classOf[Math].getName
          s"$mathClass.${exactMathMethod.get}($eval1, $eval2)"
        } else {
          s"$eval1 $symbol $eval2"
        }
        s"""
           |${ev.value} = $operation;
         """.stripMargin
      })
    case DoubleType | FloatType =>
      // When Double/Float overflows, there can be 2 cases:
      // - precision loss: according to SQL standard, the number is truncated;
      // - returns (+/-)Infinite: same behavior also other DBs have (eg. Postgres)
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
           |${ev.value} = $eval1 $symbol $eval2;
         """.stripMargin
      })
  }
}

object BinaryArithmetic {
  def unapply(e: BinaryArithmetic): Option[(Expression, Expression)] = Some((e.left, e.right))
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`+`expr2`.",
  examples = """
    Examples:
      > SELECT 1 _FUNC_ 2;
       3
  """,
  since = "1.0.0")
case class Add(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends BinaryArithmetic {
  val test_value = 1

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def decimalMethod: String = "$plus"

  override def calendarIntervalMethod: String = if (failOnError) "addExact" else "add"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = dataType match {
    case CalendarIntervalType if failOnError =>
      IntervalUtils.addExact(
        input1.asInstanceOf[CalendarInterval], input2.asInstanceOf[CalendarInterval])
    case CalendarIntervalType =>
      IntervalUtils.add(
        input1.asInstanceOf[CalendarInterval], input2.asInstanceOf[CalendarInterval])
    case _ => numeric.plus(input1, input2)
  }

  override def exactMathMethod: Option[String] = Some("addExact")
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`-`expr2`.",
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 1;
       1
  """,
  since = "1.0.0")
case class Subtract(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends BinaryArithmetic {

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  override def decimalMethod: String = "$minus"

  override def calendarIntervalMethod: String = if (failOnError) "subtractExact" else "subtract"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = dataType match {
    case CalendarIntervalType if failOnError =>
      IntervalUtils.subtractExact(
        input1.asInstanceOf[CalendarInterval], input2.asInstanceOf[CalendarInterval])
    case CalendarIntervalType =>
      IntervalUtils.subtract(
        input1.asInstanceOf[CalendarInterval], input2.asInstanceOf[CalendarInterval])
    case _ => numeric.minus(input1, input2)
  }

  override def exactMathMethod: Option[String] = Some("subtractExact")
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`*`expr2`.",
  examples = """
    Examples:
      > SELECT 2 _FUNC_ 3;
       6
  """,
  since = "1.0.0")
case class Multiply(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends BinaryArithmetic {

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"
  override def decimalMethod: String = "$times"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = numeric.times(input1, input2)

  override def exactMathMethod: Option[String] = Some("multiplyExact")
}

// Common base trait for Divide and Remainder, since these two classes are almost identical
trait DivModLike extends BinaryArithmetic {

  protected def decimalToDataTypeCodeGen(decimalResult: String): String = decimalResult

  override def nullable: Boolean = true

  private lazy val isZero: Any => Boolean = right.dataType match {
    case _: DecimalType => x => x.asInstanceOf[Decimal].isZero
    case _ => x => x == 0
  }

  final override def eval(input: InternalRow): Any = {
    // evaluate right first as we have a chance to skip left if right is 0
    val input2 = right.eval(input)
    if (input2 == null || (!failOnError && isZero(input2))) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        if (isZero(input2)) {
          // when we reach here, failOnError must bet true.
          throw new ArithmeticException("divide by zero")
        }
        evalOperation(input1, input2)
      }
    }
  }

  def evalOperation(left: Any, right: Any): Any

  /**
   * Special case handling due to division/remainder by 0 => null or ArithmeticException.
   */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val operandsDataType = left.dataType
    val isZero = if (operandsDataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val javaType = CodeGenerator.javaType(dataType)
    val operation = if (operandsDataType.isInstanceOf[DecimalType]) {
      decimalToDataTypeCodeGen(s"${eval1.value}.$decimalMethod(${eval2.value})")
    } else {
      s"($javaType)(${eval1.value} $symbol ${eval2.value})"
    }
    // evaluate right first as we have a chance to skip left if right is 0
    if (!left.nullable && !right.nullable) {
      val divByZero = if (failOnError) {
        "throw new ArithmeticException(\"divide by zero\");"
      } else {
        s"${ev.isNull} = true;"
      }
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if ($isZero) {
          $divByZero
        } else {
          ${eval1.code}
          ${ev.value} = $operation;
        }""")
    } else {
      val nullOnErrorCondition = if (failOnError) "" else s" || $isZero"
      val failOnErrorBranch = if (failOnError) {
        s"""if ($isZero) throw new ArithmeticException("divide by zero");"""
      } else {
        ""
      }
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (${eval2.isNull}$nullOnErrorCondition) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            $failOnErrorBranch
            ${ev.value} = $operation;
          }
        }""")
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`/`expr2`. It always performs floating point division.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 2;
       1.5
      > SELECT 2L _FUNC_ 2L;
       1.0
  """,
  since = "1.0.0")
// scalastyle:on line.size.limit
case class Divide(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends DivModLike {

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def inputType: AbstractDataType = TypeCollection(DoubleType, DecimalType)

  override def symbol: String = "/"
  override def decimalMethod: String = "$div"

  private lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
  }

  override def evalOperation(left: Any, right: Any): Any = div(left, right)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Divide `expr1` by `expr2`. It returns NULL if an operand is NULL or `expr2` is 0. The result is casted to long.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 2;
       1
  """,
  since = "3.0.0")
// scalastyle:on line.size.limit
case class IntegralDivide(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends DivModLike {

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def inputType: AbstractDataType = TypeCollection(LongType, DecimalType)

  override def dataType: DataType = LongType

  override def symbol: String = "/"
  override def decimalMethod: String = "quot"
  override def decimalToDataTypeCodeGen(decimalResult: String): String = s"$decimalResult.toLong()"
  override def sqlOperator: String = "div"

  private lazy val div: (Any, Any) => Any = {
    val integral = left.dataType match {
      case i: IntegralType =>
        i.integral.asInstanceOf[Integral[Any]]
      case d: DecimalType =>
        d.asIntegral.asInstanceOf[Integral[Any]]
    }
    (x, y) => {
      val res = integral.quot(x, y)
      if (res == null) {
        null
      } else {
        integral.asInstanceOf[Integral[Any]].toLong(res)
      }
    }
  }

  override def evalOperation(left: Any, right: Any): Any = div(left, right)
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the remainder after `expr1`/`expr2`.",
  examples = """
    Examples:
      > SELECT 2 % 1.8;
       0.2
      > SELECT MOD(2, 1.8);
       0.2
  """,
  since = "1.0.0")
case class Remainder(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends DivModLike {

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"
  override def decimalMethod: String = "remainder"
  override def toString: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(sqlOperator) match {
      case operator if operator == sqlOperator => s"($left $sqlOperator $right)"
      case funcName => s"$funcName($left, $right)"
    }
  }
  override def sql: String = {
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(sqlOperator) match {
      case operator if operator == sqlOperator => s"(${left.sql} $sqlOperator ${right.sql})"
      case funcName => s"$funcName(${left.sql}, ${right.sql})"
    }
  }

  private lazy val mod: (Any, Any) => Any = dataType match {
    // special cases to make float/double primitive types faster
    case DoubleType =>
      (left, right) => left.asInstanceOf[Double] % right.asInstanceOf[Double]
    case FloatType =>
      (left, right) => left.asInstanceOf[Float] % right.asInstanceOf[Float]

    // catch-all cases
    case i: IntegralType =>
      val integral = i.integral.asInstanceOf[Integral[Any]]
      (left, right) => integral.rem(left, right)
    case i: FractionalType => // should only be DecimalType for now
      val integral = i.asIntegral.asInstanceOf[Integral[Any]]
      (left, right) => integral.rem(left, right)
  }

  override def evalOperation(left: Any, right: Any): Any = mod(left, right)
}

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns the positive value of `expr1` mod `expr2`.",
  examples = """
    Examples:
      > SELECT _FUNC_(10, 3);
       1
      > SELECT _FUNC_(-10, 3);
       2
  """,
  since = "1.5.0")
case class Pmod(
    left: Expression,
    right: Expression,
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends BinaryArithmetic {

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def toString: String = s"pmod($left, $right)"

  override def symbol: String = "pmod"

  protected def checkTypesInternal(t: DataType): TypeCheckResult =
    TypeUtils.checkForNumericExpr(t, "pmod")

  override def inputType: AbstractDataType = NumericType

  override def nullable: Boolean = true

  private lazy val isZero: Any => Boolean = right.dataType match {
    case _: DecimalType => x => x.asInstanceOf[Decimal].isZero
    case _ => x => x == 0
  }

  final override def eval(input: InternalRow): Any = {
    // evaluate right first as we have a chance to skip left if right is 0
    val input2 = right.eval(input)
    if (input2 == null || (!failOnError && isZero(input2))) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        if (isZero(input2)) {
          // when we reach here, failOnError must bet true.
          throw new ArithmeticException("divide by zero")
        }
        input1 match {
          case i: Integer => pmod(i, input2.asInstanceOf[java.lang.Integer])
          case l: Long => pmod(l, input2.asInstanceOf[java.lang.Long])
          case s: Short => pmod(s, input2.asInstanceOf[java.lang.Short])
          case b: Byte => pmod(b, input2.asInstanceOf[java.lang.Byte])
          case f: Float => pmod(f, input2.asInstanceOf[java.lang.Float])
          case d: Double => pmod(d, input2.asInstanceOf[java.lang.Double])
          case d: Decimal => pmod(d, input2.asInstanceOf[Decimal])
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val remainder = ctx.freshName("remainder")
    val javaType = CodeGenerator.javaType(dataType)

    val result = dataType match {
      case DecimalType.Fixed(_, _) =>
        val decimalAdd = "$plus"
        s"""
          $javaType $remainder = ${eval1.value}.remainder(${eval2.value});
          if ($remainder.compare(new org.apache.spark.sql.types.Decimal().set(0)) < 0) {
            ${ev.value}=($remainder.$decimalAdd(${eval2.value})).remainder(${eval2.value});
          } else {
            ${ev.value}=$remainder;
          }
        """
      // byte and short are casted into int when add, minus, times or divide
      case ByteType | ShortType =>
        s"""
          $javaType $remainder = ($javaType)(${eval1.value} % ${eval2.value});
          if ($remainder < 0) {
            ${ev.value}=($javaType)(($remainder + ${eval2.value}) % ${eval2.value});
          } else {
            ${ev.value}=$remainder;
          }
        """
      case _ =>
        s"""
          $javaType $remainder = ${eval1.value} % ${eval2.value};
          if ($remainder < 0) {
            ${ev.value}=($remainder + ${eval2.value}) % ${eval2.value};
          } else {
            ${ev.value}=$remainder;
          }
        """
    }

    // evaluate right first as we have a chance to skip left if right is 0
    if (!left.nullable && !right.nullable) {
      val divByZero = if (failOnError) {
        "throw new ArithmeticException(\"divide by zero\");"
      } else {
        s"${ev.isNull} = true;"
      }
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if ($isZero) {
          $divByZero
        } else {
          ${eval1.code}
          $result
        }""")
    } else {
      val nullOnErrorCondition = if (failOnError) "" else s" || $isZero"
      val failOnErrorBranch = if (failOnError) {
        s"""if ($isZero) throw new ArithmeticException("divide by zero");"""
      } else {
        ""
      }
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (${eval2.isNull}$nullOnErrorCondition) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            $failOnErrorBranch
            $result
          }
        }""")
    }
  }

  private def pmod(a: Int, n: Int): Int = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Long, n: Long): Long = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Byte, n: Byte): Byte = {
    val r = a % n
    if (r < 0) {((r + n) % n).toByte} else r.toByte
  }

  private def pmod(a: Double, n: Double): Double = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Short, n: Short): Short = {
    val r = a % n
    if (r < 0) {((r + n) % n).toShort} else r.toShort
  }

  private def pmod(a: Float, n: Float): Float = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Decimal, n: Decimal): Decimal = {
    val r = a % n
    if (r != null && r.compare(Decimal.ZERO) < 0) {(r + n) % n} else r
  }

  override def sql: String = s"$prettyName(${left.sql}, ${right.sql})"
}

/**
 * A function that returns the least value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns the least value of all parameters, skipping null values.",
  examples = """
    Examples:
      > SELECT _FUNC_(10, 9, 2, 4, 3);
       2
  """,
  since = "1.5.0")
case class Least(children: Seq[Expression]) extends ComplexTypeMergingExpression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least two arguments")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got LEAST(${children.map(_.dataType.catalogString).mkString(", ")}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    }
  }

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.lt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    ev.isNull = JavaCode.isNullGlobal(ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, ev.isNull))
    val evals = evalChildren.map(eval =>
      s"""
         |${eval.code}
         |${ctx.reassignIfSmaller(dataType, ev, eval)}
      """.stripMargin
    )

    val resultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "least",
      extraArguments = Seq(resultType -> ev.value),
      returnType = resultType,
      makeSplitFunction = body =>
        s"""
          |$body
          |return ${ev.value};
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))
    ev.copy(code =
      code"""
         |${ev.isNull} = true;
         |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |$codes
      """.stripMargin)
  }
}

/**
 * A function that returns the greatest value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns the greatest value of all parameters, skipping null values.",
  examples = """
    Examples:
      > SELECT _FUNC_(10, 9, 2, 4, 3);
       10
  """,
  since = "1.5.0")
case class Greatest(children: Seq[Expression]) extends ComplexTypeMergingExpression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least two arguments")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got GREATEST(${children.map(_.dataType.catalogString).mkString(", ")}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    }
  }

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.gt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    ev.isNull = JavaCode.isNullGlobal(ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, ev.isNull))
    val evals = evalChildren.map(eval =>
      s"""
         |${eval.code}
         |${ctx.reassignIfGreater(dataType, ev, eval)}
      """.stripMargin
    )

    val resultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "greatest",
      extraArguments = Seq(resultType -> ev.value),
      returnType = resultType,
      makeSplitFunction = body =>
        s"""
           |$body
           |return ${ev.value};
        """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))
    ev.copy(code =
      code"""
         |${ev.isNull} = true;
         |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |$codes
      """.stripMargin)
  }
}
