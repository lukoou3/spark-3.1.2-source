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

import java.nio.ByteBuffer
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class UnsafeRowWriterSuite extends SparkFunSuite {

  def checkDecimalSizeInBytes(decimal: Decimal, numBytes: Int): Unit = {
    assert(decimal.toJavaBigDecimal.unscaledValue().toByteArray.length == numBytes)
  }

  test("ByteBuffer"){
    /**
     * ByteBuffer默认是BIG_ENDIAN
     * 可以用ByteBuffer仿照UnsafeRowWriter和UnsafeRow实现UnsafeRow，在flink大数据量时会优化很多gc
     */
    val b = 20.asInstanceOf[Byte]
    val buffer = ByteBuffer.allocateDirect(256)
    buffer.put(b)
    buffer.putInt(10)
    buffer.putInt(20)
    buffer.put(b)
    println(buffer.getInt())
  }

  /**
   * 我看底层用的是小端
   */
  test("testUnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(4)
    rowWriter.resetRowWriter()
    rowWriter.write(0, 10) // int
    rowWriter.write(1, UTF8String.fromString("Hello spark")) // str
    rowWriter.write(2, UTF8String.fromString("Hello flink")) // str
    rowWriter.write(3, 3333333555555555L) // long
    val row = rowWriter.getRow()
    var i = 0
    while (i < 4){
      println(i + " is null:" + row.isNullAt(i))
      i+=1
    }
    assert(row.getInt(0) == 10)
    assert(row.getString(1) == "Hello spark")
    assert(row.getString(2) == "Hello flink")
    assert(row.getLong(3) == 3333333555555555L)

    println("-" * 40)
    // 像这种str字段依赖cursor的不能多次写，否则浪费内存

    rowWriter.reset()
    rowWriter.setNullAt(0) // int
    rowWriter.write(1, UTF8String.fromString("Hello spark")) // str
    rowWriter.setNullAt(2) // str
    rowWriter.write(3, 3333333555555555L) // long
    i = 0
    while (i < 4){
      println(i + " is null:" + row.isNullAt(i))
      i+=1
    }
    println(row.getInt(0))
    println(row.getUTF8String(1))
    println(row.getUTF8String(2))
    println(row.getLong(3))
  }

  test("SPARK-25538: zero-out all bits for decimals") {
    val decimal1 = Decimal(0.431)
    decimal1.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal1, 8)

    val decimal2 = Decimal(123456789.1232456789)
    decimal2.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal2, 11)
    // On an UnsafeRowWriter we write decimal2 first and then decimal1
    val unsafeRowWriter1 = new UnsafeRowWriter(1)
    unsafeRowWriter1.resetRowWriter()
    unsafeRowWriter1.write(0, decimal2, decimal2.precision, decimal2.scale)
    unsafeRowWriter1.reset()
    unsafeRowWriter1.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res1 = unsafeRowWriter1.getRow
    // On a second UnsafeRowWriter we write directly decimal1
    val unsafeRowWriter2 = new UnsafeRowWriter(1)
    unsafeRowWriter2.resetRowWriter()
    unsafeRowWriter2.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res2 = unsafeRowWriter2.getRow
    // The two rows should be the equal
    assert(res1 == res2)
  }

  test("write and get calendar intervals through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.write(0, null.asInstanceOf[CalendarInterval])
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getInterval(0) === null)
    val interval = new CalendarInterval(0, 1, 0)
    rowWriter.write(1, interval)
    assert(rowWriter.getRow.getInterval(1) === interval)
  }
}
