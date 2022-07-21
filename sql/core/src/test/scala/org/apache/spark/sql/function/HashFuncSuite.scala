package org.apache.spark.sql.function

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql._
import org.apache.spark.sql.function.HashFuncSuite._
import org.scalatest.funsuite.AnyFunSuite

import scala.beans.BeanProperty

class HashFuncSuite extends AnyFunSuite{

  test(""){
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val datas = Seq(
      ("aaa", 20),
      ("bbb", 22),
      ("ccc", 24),
      ("ddd", 24)
    )

    val df = spark.createDataFrame(datas).toDF("name", "age")

    // 输入参数是一个
    //df.selectExpr("md5(name, age) md5").show(false)
    df.selectExpr("md5(name) md5").show(false)

    //df.show()


    spark.stop()
  }

  test("aa"){
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //import spark.implicits._

    val datas = Seq(
      ("aaa", 20),
      ("bbb", 22),
      ("ccc", 24),
      ("ddd", 24)
    )

    val df = spark.createDataFrame(datas).toDF("name", "age")

    /**
     * JavaTypeInference.deserializerFor(beanClass):java类的反序列化
     *    catalyst.expressions.objects.NewInstance：cls(Nil,调用的无参的构造函数),arguments(),dataType
     *    InitializeJavaBean(newInstance, setters)： 设置属性
     * ScalaReflection.deserializerForType(tpe):scala类型的反射, Product类型调用的带参数的构造器
     *
     *
     * [[org.apache.spark.sql.catalyst.expressions.objects.InitializeJavaBean]]
     * [[org.apache.spark.sql.catalyst.expressions.objects.NewInstance]]
     */
    //df.as[People]
    df.as[People2](ExpressionEncoder[People2]())
    //df.as[People3](ExpressionEncoder[People3]()).foreach{ data =>
    df.as[People3](ExpressionEncoder.javaBean(classOf[People3])).foreach{ data =>
      println(data)
    }

    //df.show()


    spark.stop()
  }

}

object HashFuncSuite{
  case class People(name:String, age:Int){
    println(name)
  }

  case class People2(name:String, age:Option[Int]){
    println(name)
  }

  class People3 extends Serializable {
    @BeanProperty
    var name:String=_
    @BeanProperty
    var age:Int=_

    override def toString = s"People3(name=$name, age=$age)"
  }
}