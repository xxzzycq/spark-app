package com.xxzzycq.example.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by yangchangqi on 2018/3/5.
  */
object RDDToDataSetExample {
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDDToDataSetExample")
      .getOrCreate()

    runInferSchemaExample(spark)
    runProgrammaticSchemaExample(spark)

    spark.stop()
  }
  /**
   * 第一种（使用反射推断Schema）方法是使用反射去推断一个包含指定的对象类型的 RDD 的 Schema.
   * 在你的 Spark 应用程序中当你已知 Schema 时这个基于方法的反射可以让你的代码更简洁.
   * Spark SQL 的 Scala 接口支持自动转换一个包含 case classes 的 RDD 为 DataFrame.
   * Case class 定义了表的 Schema.Case class 的参数名使用反射读取并且成为了列名.
   * Case class 也可以是嵌套的或者包含像 Seq 或者 Array 这样的复杂类型.这个 RDD 能够被隐式转换成一个 DataFrame 然后被注册为一个表.
   * 表可以用于后续的 SQL 语句.
   */
  private def runInferSchemaExample(spark : SparkSession): Unit = {
    // $example on:schema_inferring$
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("spark-app/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).toInt))
      .toDF

    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
  }

  /**
   * 第二种（以编程的方式指定Schema）用于创建 Dataset 的方法是通过一个允许你构造一个 Schema 然后把它应用到一个已存在的 RDD 的编程接口.
   * 然而这种方法更繁琐, 当列和它们的类型知道运行时都是未知时它允许你去构造 Dataset.
   * 当 case class 不能够在执行之前被定义（例如, records 记录的结构在一个 string 字符串中被编码了, 或者一个 text 文本 dataset 将被解析并且不同的用户投影的字段是不一样的）.
   * 一个 DataFrame 可以使用下面的三步以编程的方式来创建.
     1. 从原始的 RDD 创建 RDD 的 Row（行）;
     2. Step 1 被创建后, 创建 Schema 表示一个 StructType 匹配 RDD 中的 Row（行）的结构.
     3. 通过 SparkSession 提供的 createDataFrame 方法应用 Schema 到 RDD 的 RowS（行）.
   */
  private def runProgrammaticSchemaExample(spark : SparkSession): Unit = {
    import spark.implicits._
    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("spark-app/src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim) )

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()
  }
}
