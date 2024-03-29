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

package org.apache.spark.sql

import java.lang.reflect.ParameterizedType

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try
import org.apache.spark.annotation.Stable
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.{JavaTypeInference, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
//import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedAggregateFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils

/**
 * Functions for registering user-defined functions. Use `SparkSession.udf` to access this:
 *
 * {{{
 *   spark.udf
 * }}}
 *
 * @since 1.3.0
 */
@Stable
class UDFRegistration private[sql] (functionRegistry: FunctionRegistry) extends Logging {

  import UDFRegistration._

  protected[sql] def registerPython(name: String, udf: UserDefinedPythonFunction): Unit = {
    log.debug(
      s"""
        | Registering new PythonUDF:
        | name: $name
        | command: ${udf.func.command.toSeq}
        | envVars: ${udf.func.envVars}
        | pythonIncludes: ${udf.func.pythonIncludes}
        | pythonExec: ${udf.func.pythonExec}
        | dataType: ${udf.dataType}
        | pythonEvalType: ${PythonEvalType.toString(udf.pythonEvalType)}
        | udfDeterministic: ${udf.udfDeterministic}
      """.stripMargin)

    functionRegistry.createOrReplaceTempFunction(name, udf.builder)
  }

  /**
   * UserDefinedAggregateFunction 3.0之后被标记为废弃了, 推荐使用functions.udaf(agg)注册udaf
   * 2.4.7官网上不安全的udaf还是这个UserDefinedAggregateFunction, 安全类型是org.apache.spark.sql.expressions.Aggregator
   * 3.0之后dateset和dateframe推荐都通过Aggregator注册
   *
   * 知道为啥要启用UserDefinedAggregateFunction了:
   *  UserDefinedAggregateFunction被包装成ScalaUDAF, ScalaUDAF对聚合缓冲区和聚合函数输入的参数都会进行序列化和反序列化转换
   *  只参数转换性能还能接受, 关键是聚合变量每次也都转换, 这样效率可能很低, 比如map转换时 org.apache.spark.sql.catalyst.CatalystTypeConverters.MapConverter
   *  sql中使用key数组和value数组存map, 每次转换都要新建map, 性能很低:convertedKeys.zip(convertedValues).toMap
   *  你就算在代码中使用可变类型的map, 他每次都会反序列化成不可变的map。convertedKeys.zip(convertedValues).toMap
   *
   * 在看看使用Aggregator的方式[[functions.udaf(agg)]]:
   *  把Aggregator加入inputEncoder信息生成[[UserDefinedAggregator]]
   *  register UserDefinedAggregator时调用udaf.scalaAggregator生成[[ScalaAggregator]]
   *  ScalaAggregator继承自TypedImperativeAggregate[BUF],
   *  update时只会转换参数, 内建的collect_list和collect_set继承自Collect[mutable.ArrayBuffer[Any]]继承自TypedImperativeAggregate
   *  聚合缓冲区也会序列化和反序列化, 主要是在读取shuffle合并分区数据时, TypedImperativeAggregate的工作流程可以去看他的注释, 写的很详细
   *  TypedImperativeAggregate可以使用任意的java对象作为内部缓冲区,看TypedImperativeAggregate的注释可以在窗口函数中使用TypedImperativeAggregate函数
   *  带有TypeDimOperativeAggregate函数的SQL计划用于基于排序的聚合，而不是基于哈希的聚合，因为TypeDimOperativeAggregate使用BinaryType作为聚合缓冲区的存储格式，而基于哈希的聚合不支持这种格式。
   *  基于散列的聚合只支持可变类型的聚合缓冲区（如LongType、IntType，它们具有固定的长度，并且可以在UnsafeRow中进行适当的变异）。
   *
   *
   * Registers a user-defined aggregate function (UDAF).
   *
   * @param name the name of the UDAF.
   * @param udaf the UDAF needs to be registered.
   * @return the registered UDAF.
   *
   * @since 1.5.0
   * @deprecated this method and the use of UserDefinedAggregateFunction are deprecated.
   * Aggregator[IN, BUF, OUT] should now be registered as a UDF via the functions.udaf(agg) method.
   */
  @deprecated("Aggregator[IN, BUF, OUT] should now be registered as a UDF" +
    " via the functions.udaf(agg) method.", "3.0.0")
  def register(name: String, udaf: UserDefinedAggregateFunction): UserDefinedAggregateFunction = {
    def builder(children: Seq[Expression]) = ScalaUDAF(children, udaf)
    functionRegistry.createOrReplaceTempFunction(name, builder)
    udaf
  }

  /**
   * udf和udaf都可以注册
   *
   * Registers a user-defined function (UDF), for a UDF that's already defined using the Dataset
   * API (i.e. of type UserDefinedFunction). To change a UDF to nondeterministic, call the API
   * `UserDefinedFunction.asNondeterministic()`. To change a UDF to nonNullable, call the API
   * `UserDefinedFunction.asNonNullable()`.
   *
   * Example:
   * {{{
   *   val foo = udf(() => Math.random())
   *   spark.udf.register("random", foo.asNondeterministic())
   *
   *   val bar = udf(() => "bar")
   *   spark.udf.register("stringLit", bar.asNonNullable())
   * }}}
   *
   * @param name the name of the UDF.
   * @param udf the UDF needs to be registered.
   * @return the registered UDF.
   *
   * @since 2.2.0
   */
  def register(name: String, udf: UserDefinedFunction): UserDefinedFunction = {
    udf match {
      case udaf: UserDefinedAggregator[_, _, _] =>
        def builder(children: Seq[Expression]) = udaf.scalaAggregator(children)
        functionRegistry.createOrReplaceTempFunction(name, builder)
        udf
      case _ =>
        def builder(children: Seq[Expression]) = udf.apply(children.map(Column.apply) : _*).expr
        functionRegistry.createOrReplaceTempFunction(name, builder)
        udf
    }
  }

  // scalastyle:off line.size.limit

  /* register 0-22 were generated by this script

    (0 to 22).foreach { x =>
      val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
      val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
      val inputEncoders = (1 to x).foldRight("Nil")((i, s) => {s"Try(ExpressionEncoder[A$i]()).toOption :: $s"})
      println(s"""
        |/**
        | * Registers a deterministic Scala closure of $x arguments as user-defined function (UDF).
        | * @tparam RT return type of UDF.
        | * @since 1.3.0
        | */
        |def register[$typeTags](name: String, func: Function$x[$types]): UserDefinedFunction = {
        |  val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
        |  val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
        |  val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = $inputEncoders
        |  val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
        |  val finalUdf = if (nullable) udf else udf.asNonNullable()
        |  def builder(e: Seq[Expression]) = if (e.length == $x) {
        |    finalUdf.createScalaUDF(e)
        |  } else {
        |    throw new AnalysisException("Invalid number of arguments for function " + name +
        |      ". Expected: $x; Found: " + e.length)
        |  }
        |  functionRegistry.createOrReplaceTempFunction(name, builder)
        |  finalUdf
        |}""".stripMargin)
    }

    (0 to 22).foreach { i =>
      val extTypeArgs = (0 to i).map(_ => "_").mkString(", ")
      val anyTypeArgs = (0 to i).map(_ => "Any").mkString(", ")
      val anyCast = s".asInstanceOf[UDF$i[$anyTypeArgs]]"
      val anyParams = (1 to i).map(_ => "_: Any").mkString(", ")
      val version = if (i == 0) "2.3.0" else "1.3.0"
      val funcCall = if (i == 0) s"() => f$anyCast.call($anyParams)" else s"f$anyCast.call($anyParams)"
      println(s"""
        |/**
        | * Register a deterministic Java UDF$i instance as user-defined function (UDF).
        | * @since $version
        | */
        |def register(name: String, f: UDF$i[$extTypeArgs], returnType: DataType): Unit = {
        |  val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
        |  val func = $funcCall
        |  def builder(e: Seq[Expression]) = if (e.length == $i) {
        |    ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
        |  } else {
        |    throw new AnalysisException("Invalid number of arguments for function " + name +
        |      ". Expected: $i; Found: " + e.length)
        |  }
        |  functionRegistry.createOrReplaceTempFunction(name, builder)
        |}""".stripMargin)
    }
    */

  /**
   * Registers a deterministic Scala closure of 0 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag](name: String, func: Function0[RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 0) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 0; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 1 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag](name: String, func: Function1[A1, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 1) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 1; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 2 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag](name: String, func: Function2[A1, A2, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 2) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 2; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 3 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](name: String, func: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 3) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 3; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 4 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](name: String, func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 4) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 4; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 5 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](name: String, func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 5) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 5; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 6 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](name: String, func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 6) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 6; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 7 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](name: String, func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 7) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 7; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 8 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](name: String, func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 8) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 8; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 9 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](name: String, func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 9) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 9; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 10 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](name: String, func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 10) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 10; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 11 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag](name: String, func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 11) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 11; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 12 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag](name: String, func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 12) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 12; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 13 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag](name: String, func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 13) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 13; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 14 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag](name: String, func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 14) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 14; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 15 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag](name: String, func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 15) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 15; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 16 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag](name: String, func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 16) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 16; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 17 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag](name: String, func: Function17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 17) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 17; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 18 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag](name: String, func: Function18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 18) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 18; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 19 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag](name: String, func: Function19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Try(ExpressionEncoder[A19]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 19) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 19; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 20 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag](name: String, func: Function20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Try(ExpressionEncoder[A19]()).toOption :: Try(ExpressionEncoder[A20]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 20) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 20; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 21 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag](name: String, func: Function21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Try(ExpressionEncoder[A19]()).toOption :: Try(ExpressionEncoder[A20]()).toOption :: Try(ExpressionEncoder[A21]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 21) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 21; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  /**
   * Registers a deterministic Scala closure of 22 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   * @since 1.3.0
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag, A22: TypeTag](name: String, func: Function22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, RT]): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val ScalaReflection.Schema(dataType, nullable) = outputEncoder.map(outputSchema).getOrElse(ScalaReflection.schemaFor[RT])
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Try(ExpressionEncoder[A2]()).toOption :: Try(ExpressionEncoder[A3]()).toOption :: Try(ExpressionEncoder[A4]()).toOption :: Try(ExpressionEncoder[A5]()).toOption :: Try(ExpressionEncoder[A6]()).toOption :: Try(ExpressionEncoder[A7]()).toOption :: Try(ExpressionEncoder[A8]()).toOption :: Try(ExpressionEncoder[A9]()).toOption :: Try(ExpressionEncoder[A10]()).toOption :: Try(ExpressionEncoder[A11]()).toOption :: Try(ExpressionEncoder[A12]()).toOption :: Try(ExpressionEncoder[A13]()).toOption :: Try(ExpressionEncoder[A14]()).toOption :: Try(ExpressionEncoder[A15]()).toOption :: Try(ExpressionEncoder[A16]()).toOption :: Try(ExpressionEncoder[A17]()).toOption :: Try(ExpressionEncoder[A18]()).toOption :: Try(ExpressionEncoder[A19]()).toOption :: Try(ExpressionEncoder[A20]()).toOption :: Try(ExpressionEncoder[A21]()).toOption :: Try(ExpressionEncoder[A22]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(func, dataType, inputEncoders, outputEncoder).withName(name)
    val finalUdf = if (nullable) udf else udf.asNonNullable()
    def builder(e: Seq[Expression]) = if (e.length == 22) {
      finalUdf.createScalaUDF(e)
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 22; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
    finalUdf
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Register a Java UDF class using reflection, for use from pyspark
   *
   * @param name   udf name
   * @param className   fully qualified class name of udf
   * @param returnDataType  return type of udf. If it is null, spark would try to infer
   *                        via reflection.
   */
  private[sql] def registerJava(name: String, className: String, returnDataType: DataType): Unit = {

    try {
      val clazz = Utils.classForName[AnyRef](className)
      val udfInterfaces = clazz.getGenericInterfaces
        .filter(_.isInstanceOf[ParameterizedType])
        .map(_.asInstanceOf[ParameterizedType])
        .filter(e => e.getRawType.isInstanceOf[Class[_]] && e.getRawType.asInstanceOf[Class[_]].getCanonicalName.startsWith("org.apache.spark.sql.api.java.UDF"))
      if (udfInterfaces.length == 0) {
        throw new AnalysisException(s"UDF class $className doesn't implement any UDF interface")
      } else if (udfInterfaces.length > 1) {
        throw new AnalysisException(s"It is invalid to implement multiple UDF interfaces, UDF class $className")
      } else {
        try {
          val udf = clazz.getConstructor().newInstance()
          val udfReturnType = udfInterfaces(0).getActualTypeArguments.last
          var returnType = returnDataType
          if (returnType == null) {
            returnType = JavaTypeInference.inferDataType(udfReturnType)._1
          }

          udfInterfaces(0).getActualTypeArguments.length match {
            case 1 => register(name, udf.asInstanceOf[UDF0[_]], returnType)
            case 2 => register(name, udf.asInstanceOf[UDF1[_, _]], returnType)
            case 3 => register(name, udf.asInstanceOf[UDF2[_, _, _]], returnType)
            case 4 => register(name, udf.asInstanceOf[UDF3[_, _, _, _]], returnType)
            case 5 => register(name, udf.asInstanceOf[UDF4[_, _, _, _, _]], returnType)
            case 6 => register(name, udf.asInstanceOf[UDF5[_, _, _, _, _, _]], returnType)
            case 7 => register(name, udf.asInstanceOf[UDF6[_, _, _, _, _, _, _]], returnType)
            case 8 => register(name, udf.asInstanceOf[UDF7[_, _, _, _, _, _, _, _]], returnType)
            case 9 => register(name, udf.asInstanceOf[UDF8[_, _, _, _, _, _, _, _, _]], returnType)
            case 10 => register(name, udf.asInstanceOf[UDF9[_, _, _, _, _, _, _, _, _, _]], returnType)
            case 11 => register(name, udf.asInstanceOf[UDF10[_, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 12 => register(name, udf.asInstanceOf[UDF11[_, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 13 => register(name, udf.asInstanceOf[UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 14 => register(name, udf.asInstanceOf[UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 15 => register(name, udf.asInstanceOf[UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 16 => register(name, udf.asInstanceOf[UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 17 => register(name, udf.asInstanceOf[UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 18 => register(name, udf.asInstanceOf[UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 19 => register(name, udf.asInstanceOf[UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 20 => register(name, udf.asInstanceOf[UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 21 => register(name, udf.asInstanceOf[UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 22 => register(name, udf.asInstanceOf[UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case 23 => register(name, udf.asInstanceOf[UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], returnType)
            case n =>
              throw new AnalysisException(s"UDF class with $n type arguments is not supported.")
          }
        } catch {
          case e @ (_: InstantiationException | _: IllegalArgumentException) =>
            throw new AnalysisException(s"Can not instantiate class $className, please make sure it has public non argument constructor")
        }
      }
    } catch {
      case e: ClassNotFoundException => throw new AnalysisException(s"Can not load class $className, please make sure it is on the classpath")
    }

  }

  /**
   * Register a Java UDAF class using reflection, for use from pyspark
   *
   * @param name     UDAF name
   * @param className    fully qualified class name of UDAF
   */
  private[sql] def registerJavaUDAF(name: String, className: String): Unit = {
    try {
      val clazz = Utils.classForName[AnyRef](className)
      if (!classOf[UserDefinedAggregateFunction].isAssignableFrom(clazz)) {
        throw new AnalysisException(s"class $className doesn't implement interface UserDefinedAggregateFunction")
      }
      val udaf = clazz.getConstructor().newInstance().asInstanceOf[UserDefinedAggregateFunction]
      register(name, udaf)
    } catch {
      case e: ClassNotFoundException => throw new AnalysisException(s"Can not load class ${className}, please make sure it is on the classpath")
      case e @ (_: InstantiationException | _: IllegalArgumentException) =>
        throw new AnalysisException(s"Can not instantiate class ${className}, please make sure it has public non argument constructor")
    }
  }

  /**
   * Register a deterministic Java UDF0 instance as user-defined function (UDF).
   * @since 2.3.0
   */
  def register(name: String, f: UDF0[_], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = () => f.asInstanceOf[UDF0[Any]].call()
    def builder(e: Seq[Expression]) = if (e.length == 0) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 0; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF1 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF1[_, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF1[Any, Any]].call(_: Any)
    def builder(e: Seq[Expression]) = if (e.length == 1) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 1; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF2 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF2[_, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF2[Any, Any, Any]].call(_: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 2) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 2; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF3 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF3[_, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF3[Any, Any, Any, Any]].call(_: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 3) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 3; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF4 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF4[_, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF4[Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 4) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 4; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF5 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF5[_, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 5) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 5; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF6 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF6[_, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF6[Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 6) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 6; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF7 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF7[_, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF7[Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 7) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 7; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF8 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF8[_, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 8) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 8; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF9 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF9[_, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 9) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 9; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF10 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF10[_, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 10) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 10; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF11 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF11[_, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 11) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 11; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF12 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 12) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 12; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF13 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 13) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 13; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF14 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 14) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 14; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF15 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 15) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 15; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF16 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 16) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 16; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF17 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 17) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 17; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF18 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 18) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 18; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF19 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 19) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 19; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF20 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 20) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 20; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF21 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 21) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 21; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  /**
   * Register a deterministic Java UDF22 instance as user-defined function (UDF).
   * @since 1.3.0
   */
  def register(name: String, f: UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType): Unit = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(returnType)
    val func = f.asInstanceOf[UDF22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any)
    def builder(e: Seq[Expression]) = if (e.length == 22) {
      ScalaUDF(func, replaced, e, Nil, udfName = Some(name))
    } else {
      throw new AnalysisException("Invalid number of arguments for function " + name +
        ". Expected: 22; Found: " + e.length)
    }
    functionRegistry.createOrReplaceTempFunction(name, builder)
  }

  // scalastyle:on line.size.limit

}

private[sql] object UDFRegistration {
  /**
   * Obtaining the schema of output encoder for `ScalaUDF`.
   *
   * As the serialization in `ScalaUDF` is for individual column, not the whole row,
   * we just take the data type of vanilla object serializer, not `serializer` which
   * is transformed somehow for top-level row.
   */
  def outputSchema(outputEncoder: ExpressionEncoder[_]): ScalaReflection.Schema = {
    ScalaReflection.Schema(outputEncoder.objSerializer.dataType,
      outputEncoder.objSerializer.nullable)
  }
}
