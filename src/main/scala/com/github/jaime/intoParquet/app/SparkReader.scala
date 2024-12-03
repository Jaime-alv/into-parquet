/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.app.SparkBuilder.spark
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.service.Common.renderPath
import org.apache.spark.sql.DataFrame

object SparkReader extends AppLogger {

    def readInferSchema(filename: String): DataFrame = {
        renderReadMessage(filename)
        spark.read
            .option("mode", "DROPMALFORMED")
            .option("columnNameOfCorruptRecord", "ERROR")
            .option("inferSchema", true)
            .option("nullValue", "NULL")
            .option("TimeStampFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]")
            .option("header", true)
            .format("csv")
            .csv(filename)
    }

    def readRawCSV(filename: String): DataFrame = {
        renderReadMessage(filename)
        spark.read
            .option("header", true)
            .option("inferSchema", false)
            .option("nullValue", "NULL")
            .csv(filename)
    }

    private def renderReadMessage(path: String): Unit = {
        logDebug(s"Read data from ${renderPath(path)}")
    }
}
