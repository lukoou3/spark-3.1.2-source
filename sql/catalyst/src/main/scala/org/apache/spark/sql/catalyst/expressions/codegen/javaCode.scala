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

import java.lang.{Boolean => JBool}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * java代码块,
 * Trait representing an opaque fragments of java code.
 */
trait JavaCode {
  def code: String
  override def toString: String = code
}

/**
 * Utility functions for creating [[JavaCode]] fragments.
 */
object JavaCode {
  /**
   * Create a java literal.
   */
  def literal(v: String, dataType: DataType): LiteralValue = dataType match {
    case BooleanType if v == "true" => TrueLiteral
    case BooleanType if v == "false" => FalseLiteral
    case _ => new LiteralValue(v, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a default literal. This is null for reference types, false for boolean types and
   * -1 for other primitive types.
   */
  def defaultLiteral(dataType: DataType): LiteralValue = {
    new LiteralValue(
      CodeGenerator.defaultValue(dataType, typedNull = true),
      CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a local java variable.
   */
  def variable(name: String, dataType: DataType): VariableValue = {
    variable(name, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a local java variable.
   */
  def variable(name: String, javaClass: Class[_]): VariableValue = {
    VariableValue(name, javaClass)
  }

  /**
   * Create a local isNull variable.
   */
  def isNullVariable(name: String): VariableValue = variable(name, BooleanType)

  /**
   * Create a global java variable.
   */
  def global(name: String, dataType: DataType): GlobalValue = {
    global(name, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a global java variable.
   */
  def global(name: String, javaClass: Class[_]): GlobalValue = {
    GlobalValue(name, javaClass)
  }

  /**
   * Create a global isNull variable.
   */
  def isNullGlobal(name: String): GlobalValue = global(name, BooleanType)

  /**
   * Create an expression fragment.
   */
  def expression(code: String, dataType: DataType): SimpleExprValue = {
    expression(code, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create an expression fragment.
   */
  def expression(code: String, javaClass: Class[_]): SimpleExprValue = {
    SimpleExprValue(code, javaClass)
  }

  /**
   * Create a isNull expression fragment.
   */
  def isNullExpression(code: String): SimpleExprValue = {
    expression(code, BooleanType)
  }

  /**
   * Create an `Inline` for Java Class name.
   */
  def javaType(javaClass: Class[_]): Inline = Inline(javaClass.getName)

  /**
   * Create an `Inline` for Java Type name.
   */
  def javaType(dataType: DataType): Inline = Inline(CodeGenerator.javaType(dataType))

  /**
   * Create an `Inline` for boxed Java Type name.
   */
  def boxedType(dataType: DataType): Inline = Inline(CodeGenerator.boxedType(dataType))
}

/**
 * 代表java代码块的接口
 * A trait representing a block of java code.
 */
trait Block extends TreeNode[Block] with JavaCode {
  import Block._

  // Returns java code string for this code block.
  override def toString: String = _marginChar match {
    case Some(c) => code.stripMargin(c).trim
    case _ => code.trim
  }

  // We could remove comments, extra whitespaces and newlines when calculating length as it is used
  // only for codegen method splitting, but SPARK-30564 showed that this is a performance critical
  // function so we decided not to do so.
  def length: Int = toString.length

  def isEmpty: Boolean = toString.isEmpty

  def nonEmpty: Boolean = !isEmpty

  // The leading prefix that should be stripped from each line.
  // By default we strip blanks or control characters followed by '|' from the line.
  var _marginChar: Option[Char] = Some('|')

  def stripMargin(c: Char): this.type = {
    _marginChar = Some(c)
    this
  }

  def stripMargin: this.type = {
    _marginChar = Some('|')
    this
  }

  /**
   * Apply a map function to each java expression codes present in this java code, and return a new
   * java code based on the mapped java expression codes.
   */
  def transformExprValues(f: PartialFunction[ExprValue, ExprValue]): this.type = {
    var changed = false

    @inline def transform(e: ExprValue): ExprValue = {
      val newE = f lift e
      if (!newE.isDefined || newE.get.equals(e)) {
        e
      } else {
        changed = true
        newE.get
      }
    }

    def doTransform(arg: Any): AnyRef = arg match {
      case e: ExprValue => transform(e)
      case Some(value) => Some(doTransform(value))
      case seq: Iterable[_] => seq.map(doTransform)
      case other: AnyRef => other
    }

    val newArgs = mapProductIterator(doTransform)
    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  // Concatenates this block with other block.
  def + (other: Block): Block = other match {
    case EmptyBlock => this
    case _ => code"$this\n$other"
  }

  override def verboseString(maxFields: Int): String = toString
  override def simpleStringWithNodeId(): String = {
    throw new UnsupportedOperationException(s"$nodeName does not implement simpleStringWithNodeId")
  }
}

object Block {

  val CODE_BLOCK_BUFFER_LENGTH: Int = 512

  /**
   * A custom string interpolator which inlines a string into code block.
   */
  implicit class InlineHelper(val sc: StringContext) extends AnyVal {
    def inline(args: Any*): Inline = {
      val inlineString = sc.raw(args: _*)
      Inline(inlineString)
    }
  }

  implicit def blocksToBlock(blocks: Seq[Block]): Block = blocks.reduceLeft(_ + _)

  implicit class BlockHelper(val sc: StringContext) extends AnyVal {
    /**
     * 一个字符串插值器，保留对JavaCode输入的引用，在其他方面表现得像Scala内置的StringContext.（）插值器，
     * 即它将处理代码部分中的转义，而不会处理输入参数中的转义。
     * A string interpolator that retains references to the `JavaCode` inputs, and behaves like
     * the Scala builtin StringContext.s() interpolator otherwise, i.e. it will treat escapes in
     * the code parts, and will not treat escapes in the input arguments.
     */
    def code(args: Any*): Block = {
      sc.checkLengths(args)
      if (sc.parts.length == 0) {
        EmptyBlock
      } else {
        args.foreach {
          case _: ExprValue | _: Inline | _: Block =>
          case _: Boolean | _: Int | _: Long | _: Float | _: Double | _: String =>
          case other => throw new IllegalArgumentException(
            s"Can not interpolate ${other.getClass.getName} into code block.")
        }

        // 折叠字面量字符串到代码部分
        val (codeParts, blockInputs) = foldLiteralArgs(sc.parts, args)
        CodeBlock(codeParts, blockInputs)
      }
    }
  }

  // Folds eagerly the literal args into the code parts.
  private def foldLiteralArgs(parts: Seq[String], args: Seq[Any]): (Seq[String], Seq[JavaCode]) = {
    val codeParts = ArrayBuffer.empty[String] // 代码部分
    val blockInputs = ArrayBuffer.empty[JavaCode] // 输入

    val strings = parts.iterator
    val inputs = args.iterator
    val buf = new StringBuilder(Block.CODE_BLOCK_BUFFER_LENGTH)

    buf.append(StringContext.treatEscapes(strings.next))
    while (strings.hasNext) {
      val input = inputs.next
      input match {
        case _: ExprValue | _: CodeBlock =>
          codeParts += buf.toString
          buf.clear
          blockInputs += input.asInstanceOf[JavaCode]
        case EmptyBlock =>
        case _ =>
          buf.append(input)
      }
      buf.append(StringContext.treatEscapes(strings.next))
    }
    codeParts += buf.toString

    (codeParts.toSeq, blockInputs.toSeq)
  }
}

/**
 * java代码块。包括一系列代码部分和该块的一些输入。实际的java代码是通过将输入嵌入代码部分生成的。
 * 在这里，我们保留Java代码的输入，而不是简单地将它们折叠为一个代码字符串，因为我们需要跟踪此代码块中的表达式（ExprValue）。
 * 在某些应用程序中，我们需要能够在不改变此代码块行为的情况下操作表达式，例如，方法拆分。
 *
 * A block of java code. Including a sequence of code parts and some inputs to this block.
 * The actual java code is generated by embedding the inputs into the code parts. Here we keep
 * inputs of `JavaCode` instead of simply folding them as a string of code, because we need to
 * track expressions (`ExprValue`) in this code block. We need to be able to manipulate the
 * expressions later without changing the behavior of this code block in some applications, e.g.,
 * method splitting.
 */
case class CodeBlock(codeParts: Seq[String], blockInputs: Seq[JavaCode]) extends Block {
  override def children: Seq[Block] =
    blockInputs.filter(_.isInstanceOf[Block]).asInstanceOf[Seq[Block]]

  override lazy val code: String = {
    val strings = codeParts.iterator
    val inputs = blockInputs.iterator
    val buf = new StringBuilder(Block.CODE_BLOCK_BUFFER_LENGTH)
    buf.append(strings.next)
    while (strings.hasNext) {
      buf.append(inputs.next)
      buf.append(strings.next)
    }
    buf.toString
  }
}

case object EmptyBlock extends Block with Serializable {
  override val code: String = ""
  override def children: Seq[Block] = Seq.empty
}

/**
 * 一段java代码片段将所有类型的输入参数内联到一个字符串中，而不跟踪java代码实例的任何引用。
 * A piece of java code snippet inlines all types of input arguments into a string without
 * tracking any reference of `JavaCode` instances.
 */
case class Inline(codeString: String) extends JavaCode {
  override val code: String = codeString
}

/**
 * 有类型的java片段, 必须是一个有效的java表达式
 * A typed java fragment that must be a valid java expression.
 */
trait ExprValue extends JavaCode {
  def javaType: Class[_]
  def isPrimitive: Boolean = javaType.isPrimitive
}

object ExprValue {
  implicit def exprValueToString(exprValue: ExprValue): String = exprValue.code
}

/**
 * simple java表达式片段, 把expr放到了()里
 * A java expression fragment.
 */
case class SimpleExprValue(expr: String, javaType: Class[_]) extends ExprValue {
  override def code: String = s"($expr)"
}

/**
 * 局部变量java表达式, code就是value
 * A local variable java expression.
 */
case class VariableValue(variableName: String, javaType: Class[_]) extends ExprValue {
  override def code: String = variableName
}

/**
 * 全局变量java表达式, code就是value
 * A global variable java expression.
 */
case class GlobalValue(value: String, javaType: Class[_]) extends ExprValue {
  override def code: String = value
}

/**
 * 字面量java表达式, code就是value
 * true和false都是固定的, 所以预定义了两个object: TrueLiteral, FalseLiteral
 * A literal java expression.
 */
class LiteralValue(val value: String, val javaType: Class[_]) extends ExprValue with Serializable {
  override def code: String = value

  override def equals(arg: Any): Boolean = arg match {
    case l: LiteralValue => l.javaType == javaType && l.value == value
    case _ => false
  }

  override def hashCode(): Int = value.hashCode() * 31 + javaType.hashCode()
}

case object TrueLiteral extends LiteralValue("true", JBool.TYPE)
case object FalseLiteral extends LiteralValue("false", JBool.TYPE)
