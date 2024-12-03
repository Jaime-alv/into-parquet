/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.GivenWhenThen

class TestSparkReadingCSV extends SparkTestBuilder with GivenWhenThen {

    private val Filename: String      = "exampleTable.csv"
    private val TimestampFile: String = "timestampConversion.csv"
    private val FilePath: String      = s"${Resources.InputTestFolder}"
    private val file: String          = s"$FilePath$Filename"

    private def buildRawData: DataFrame = {
        val data = Seq(
          Row("John", "0"),
          Row("admin", "1"),
          Row("empty", null),
          Row(null, "3")
        )
        val schema = StructType(
          Seq(
            StructField("name", StringType),
            StructField("id", StringType)
          )
        )
        buildDataFrame(data, schema)
    }

    private def expectedData: DataFrame = {
        val expectedData = Seq(
          Row("John", 0),
          Row("admin", 1),
          Row("empty", null),
          Row(null, 3)
        )
        val expectedSchema = StructType(
          Seq(
            StructField("name", StringType),
            StructField("id", IntegerType)
          )
        )
        buildDataFrame(expectedData, expectedSchema)
    }

    test("Should read in raw format") {
        Given("a raw csv file with both string and int")
        When("read in raw format")
        val df = SparkReader.readRawCSV(file)
        Then("schema should be string & string")
        val expectedSchema = StructType(
          Seq(
            StructField("name", StringType),
            StructField("id", StringType)
          )
        )
        assertResult(expectedSchema)(df.schema)
    }

    test("Should handle null values when reading raw") {
        val df = SparkReader.readRawCSV(file)
        assertDataFrameNoOrderEquals(buildRawData, df)
    }

    test("Should infer the schema") {
        Given("a raw csv file with both string and int")
        When("read inferring the schema")
        val df = SparkReader.readInferSchema(file)
        Then("schema should be string & int")
        val expectedSchema = StructType(
          Seq(
            StructField("name", StringType),
            StructField("id", IntegerType)
          )
        )
        assertResult(expectedSchema)(df.schema)
    }

    test("Should handle null values when inferring the schema") {
        val df = SparkReader.readInferSchema(file)
        assertDataFrameNoOrderEquals(expectedData, df)
    }

    test("Should infer the schema with decimals") {
        val df = SparkReader.readInferSchema(s"$FilePath$TimestampFile")
        val expectedSchema = StructType(
          List(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("start_time", TimestampType),
            StructField("end_time_with_decimals", TimestampType)
          )
        )
        assertResult(expectedSchema)(df.schema)
        assertResult(7)(df.count())
    }
}
