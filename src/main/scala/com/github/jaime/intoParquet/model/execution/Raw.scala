/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkReader.readRawCSV
import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.configuration.BasePaths
import org.apache.spark.sql.DataFrame

class Raw(_file: String, _paths: BasePaths) extends Executor with ReadAndWrite with AppLogger {

    override protected val file: String = _file
    override val paths: BasePaths       = _paths

    override def readFrom: DataFrame = {
        logInfo(s"Read raw data from $file")
        readRawCSV(absoluteInputCSVPath)
    }
}
