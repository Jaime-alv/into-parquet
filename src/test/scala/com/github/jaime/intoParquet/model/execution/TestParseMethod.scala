/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.exception.EnrichNotImplementedTypeException
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.Field
import com.github.jaime.intoParquet.model.TableDescription
import com.github.jaime.intoParquet.model.enumeration._
import com.github.jaime.intoParquet.model.execution.Parse.applySchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp

class TestParseMethod extends SparkTestBuilder {

    private val basePaths = Resources.path

    test("Should do noting as fallback") {
        val parse = new Parse("timestampConversion", basePaths, FallBackNone)
        assert(parse.cast.isSuccess)
    }

    test("Should return a failure") {
        val parse = new Parse("timestampConversion", basePaths, FallBackFail)
        val e     = parse.cast
        assert(e.isFailure)
        assertThrows[NoSchemaFoundException](e.get)
    }

    test("Should parse with infer") {
        val parse = new Parse("timestampConversion", basePaths, FallBackInfer)
        assert(parse.cast.isSuccess)
    }

    test("Should parse with raw") {
        val parse = new Parse("timestampConversion", basePaths, FallBackRaw)
        assert(parse.cast.isSuccess)
    }

    test("Should finish Ok if everything is right") {
        val parse = new Parse("exampleTable", basePaths, FallBackNone)
        assert(parse.cast.isSuccess)
    }

    test("Should be a failure if table description has wrong format but exists") {
        val parse = new Parse("wrongType", basePaths, FallBackNone)
        assume(parse.cast.isFailure)
        assertThrows[NotImplementedTypeException](parse.cast.get)
    }

    test("Should enrich the exception with file information") {
        val parse = new Parse("wrongType", basePaths, FallBackNone)
        assume(parse.cast.isFailure)
        assertThrows[EnrichNotImplementedTypeException](parse.cast.get)
        val exception = intercept[EnrichNotImplementedTypeException](parse.cast.get)
        assertResult("wrongType")(exception.file)
    }

    test(
      "Should not fail if the schema is wrong and is trying to force a conversion for a different type"
    ) {
        val parse = new Parse("badSchema", basePaths, FallBackNone)
        assume(parse.cast.isSuccess)
        val expectedData = List(
          Row(1, "none", null),
          Row(2, "john", 2345),
          Row(3, "mary", null),
          Row(4, "carrot", 1002)
        )
        val expectedSchema = StructType(
          List(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("postal_region", IntegerType)
          )
        )
        val expectedDF = buildDataFrame(expectedData, expectedSchema)
        assertDataFrameNoOrderEquals(expectedDF, parse.readFrom)
    }

    test("Should apply a schema to an all string dataframe") {
        val wrapper = new TableDescription(
            List(
                new Field("name", StringDataType),
                new Field("id", IntegerDataType)
            )
        )

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
        val rawData = buildDataFrame(data, schema)
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
        val expectedDF = buildDataFrame(expectedData, expectedSchema)
        val converted = applySchema(rawData, wrapper)
        assertDataFrameNoOrderEquals(expectedDF, converted)
    }


    private val DefaultCol: String = "data"

    private def buildRawStringData(data: List[String]): DataFrame = {
        val schema = StructType(
            List(
                StructField(DefaultCol, StringType)
            )
        )
        buildDataFrame(data.map(Row(_)), schema)
    }

    private def buildTableDescription(dataType: SQLDataType): TableDescription = {
        new TableDescription(List(new Field(DefaultCol, dataType)))
    }

    private def buildExpectedSchema(a: DataType): StructType = {
        StructType(List(StructField(DefaultCol, a)))
    }

    // Test boolean type to match expectations

    test("Should convert boolean values") {
        val inputData      = List("true", "false", "TRUE", "FALSE", "random")
        val inputDF        = buildRawStringData(inputData)
        val table          = buildTableDescription(BooleanDataType)
        val expectedData   = List(true, false, true, false, null).map(Row(_))
        val expectedSchema = buildExpectedSchema(BooleanType)
        assertSmallDataFrameDataEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    // Test Numeric types to match expectations

    test("Should apply a schema to bytes (1-byte signed integer, from -128 to 127)") {
        val inputData      = List("-1", "0", "1", "-256", "256", "string")
        val inputDf        = buildRawStringData(inputData)
        val table          = buildTableDescription(ByteDataType)
        val expectedData   = List(-1.toByte, 0.toByte, 1.toByte, null, null, null).map(Row(_))
        val expectedSchema = buildExpectedSchema(ByteType)
        assertSmallDataFrameDataEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDf, table)
        )
    }

    test("Should apply schema to short type (2-byte signed integer, from -32,768 to 32,767)") {
        val inputData = List("-10001", "0", "10001", "random", "-52768", "52768")
        val inputDF   = buildRawStringData(inputData)
        val table     = buildTableDescription(ShortDataType)
        val expectedData =
            List(-10001.toShort, 0.toShort, 10001.toShort, null, null, null).map(Row(_))
        val expectedSchema = buildExpectedSchema(ShortType)
        assertSmallDataFrameDataEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    test(
        "Should apply schema to integer type (4-byte signed integer, from -2,147,483,648 to 2,147,483,647)"
    ) {
        val inputData = List("-20241710", "0", "20241127", "example", "-3147483648", "3147483648")
        val inputDF   = buildRawStringData(inputData)
        val table     = buildTableDescription(IntegerDataType)
        val expectedData =
            List(-20241710, 0, 20241127, null, null, null).map(Row(_))
        val expectedSchema = buildExpectedSchema(IntegerType)
        assertSmallDataFrameDataEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    test(
        "Should apply schema to long integer type (8-byte signed integer, from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)"
    ) {
        val inputData = List(
            "-72036854775808",
            "0",
            "72036854775808",
            "example",
            "-119223372036854775808",
            "119223372036854775808"
        )
        val inputDF = buildRawStringData(inputData)
        val table   = buildTableDescription(LongDataType)
        val expectedData =
            List(-72036854775808L, 0L, 72036854775808L, null, null, null).map(Row(_))
        val expectedSchema = buildExpectedSchema(LongType)
        assertSmallDataFrameDataEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    // Test Date conversion to match expectations

    test("Should apply schema to date types with ISO format") {
        val inputData =
            List("2022-11-27", "1986-05-04", "1986-20-12", "20241111", "24-01-2023", "02-02-2024")
        val inputDF = buildRawStringData(inputData)
        val table   = buildTableDescription(DateDataType)
        val expectedData =
            List(Date.valueOf("2022-11-27"), Date.valueOf("1986-05-04"), null, null, null, null)
                .map(Row(_))
        val expectedSchema = buildExpectedSchema(DateType)
        assertSmallDataFrameDataEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    test("Should apply schema to timestamp types with ISO format") {
        val inputData =
            List(
                "2022-11-27",
                "1808-05-02 22:00:30",
                "17890714 18:00:00",
                "2022-09-27 18:00:00.000",
                "2004-09-04 15:11:34.123456789"
            )
        val inputDF = buildRawStringData(inputData)
        val table   = buildTableDescription(TimeStampDataType)
        val expectedData =
            List(
                Timestamp.valueOf("2022-11-27 00:00:00"),
                Timestamp.valueOf("1808-05-02 22:00:30"),
                Timestamp.valueOf("2022-09-27 18:00:00"),
                Timestamp.valueOf("2004-09-04 15:11:34.123456789"),
                null
            )
                .map(Row(_))
        val expectedSchema = buildExpectedSchema(TimestampType)
        assertSmallDataFrameDataEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    // Test decimal types to match expectations

    test("Should apply schema to float (32-bit signed single-precision floating-point type)") {
        val inputData =
            List("-57.295779513", "3.14159265359", "0.0", "1", "invalid")
        val inputDF = buildRawStringData(inputData)
        val table   = buildTableDescription(FloatDataType)
        val expectedData =
            List(-57.295779513.toFloat, 3.14159265359.toFloat, 0.0.toFloat, 1.0.toFloat, null)
                .map(Row(_))
        val expectedSchema = buildExpectedSchema(FloatType)
        assertSmallDataFrameDataEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    test("Should apply schema to double (64-bit signed double-precision floating-point type)") {
        val inputData =
            List(
                "1.6180339887498948482045868343",
                "0.0",
                "1",
                "invalid",
                "-0.5"
            )
        val inputDF = buildRawStringData(inputData)
        val table   = buildTableDescription(DoubleDataType)
        val expectedData =
            List(1.6180339887498948482045868343, 0.0, 1.0, null, -0.5)
                .map(Row(_))
        val expectedSchema = buildExpectedSchema(DoubleType)
        assertDataFrameNoOrderEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    test("Should apply decimal types (precision and scale)") {
        val inputData =
            List("-6.371111", "39.473056", "0.0", "1", "invalid")
        val inputDF = buildRawStringData(inputData)
        val table   = buildTableDescription(new DecimalDataType(8, 6))
        val expectedData =
            List(
                new BigDecimal(-6.371111),
                new BigDecimal(39.473056),
                new BigDecimal(0.0),
                new BigDecimal(1.0),
                null
            )
                .map(Row(_))
        val expectedSchema = buildExpectedSchema(new DecimalType(8, 6))
        assertDataFrameNoOrderEquals(
            buildDataFrame(expectedData, expectedSchema),
            applySchema(inputDF, table)
        )
    }

    test("Should parse all simple non-nested data types") {
        val parse = new Parse("allSimpleDataTypes", basePaths, FallBackFail)
        assume(parse.cast.isSuccess)
        val result = parse.readFrom
        val expectedData = List(
          Row(
            "example",
            true,
            Timestamp.valueOf("2024-11-10 22:01:35"),
            Date.valueOf("2024-11-27"),
            10.toByte,
            120.toByte,
            10001.toShort,
            28039.toShort,
            20240504,
            20241127,
            4147483647L,
            9147483647L,
            1.618,
            3.141592.toFloat,
            2.7182818284.toFloat,
            new java.math.BigDecimal(57.2958)
          )
        )
        val expectedSchema = StructType(
            List(
                StructField("string", StringType),
                StructField("boolean", BooleanType),
                StructField("timestamp", TimestampType),
                StructField("date", DateType),
                StructField("byte", ByteType),
                StructField("tinyint", ByteType),
                StructField("smallint", ShortType),
                StructField("short", ShortType),                
                StructField("int", IntegerType),
                StructField("integer", IntegerType),
                StructField("bigint", LongType),
                StructField("long", LongType),
                StructField("double", DoubleType),
                StructField("float", FloatType),
                StructField("real", FloatType),
                StructField("decimal", new DecimalType(8,6))                
            )
        )
        val expected = buildDataFrame(expectedData, expectedSchema)
        assertResult(expected.schema)(result.schema)
        assertDataFrameEquals(expected, result)
    }
}
