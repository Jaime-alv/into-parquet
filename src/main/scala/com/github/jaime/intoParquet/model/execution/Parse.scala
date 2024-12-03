/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.app.SparkReader.readRawCSV
import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.behaviour.Executor
import com.github.jaime.intoParquet.behaviour.ReadAndWrite
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.EnrichNotImplementedTypeException
import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.TableDescription
import com.github.jaime.intoParquet.model.enumeration.FallBack
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw
import com.github.jaime.intoParquet.model.execution.Parse.applySchema
import com.github.jaime.intoParquet.service.FileLoader.readFile
import org.apache.spark.sql.DataFrame

class Parse(_file: String, _paths: BasePaths, fallBack: FallBack)
    extends Executor
    with ReadAndWrite
    with AppLogger {

    override protected val file: String                         = _file
    override val paths: BasePaths                               = _paths
    private lazy val tableDescription: Option[TableDescription] = loadTableDescription

    override def readFrom: DataFrame = {
        logDebug("Load table description")
        val raw = readRawCSV(absoluteInputCSVPath)
        logInfo(s"Apply schema to current data")
        applySchema(raw, tableDescription.get)
    }

    override def execution(): Unit = {
        if (tableDescription.isEmpty) {
            applyFallbackMethodToCurrentFile.execution()
        } else {
            writeResult()
        }
    }

    private def loadTableDescription: Option[TableDescription] = {
        readFile(paths.absoluteInputTableDescriptionPath(file)) match {
            case Some(value) => Some(intoTableDescription(value))
            case None =>
                logWarning(s"No table description file for $file")
                None
        }
    }

    private def intoTableDescription(tableLines: List[String]): TableDescription = {
        try {
            new TableDescription(tableLines)
        } catch {
            case sqlError: NotImplementedTypeException =>
                throw new EnrichNotImplementedTypeException(
                  file = _file,
                  invalidType = sqlError.invalidType
                )
            case e: Exception => throw e
        }
    }

    private def applyFallbackMethodToCurrentFile: Executor = {
        logDebug(s"Apply fallback method ${fallBack.toString}")
        fallBack match {
            case FallBackRaw   => new Raw(file, paths)
            case FallBackInfer => new Infer(file, paths)
            case FallBackFail  => new Fail(file)
            case FallBackNone  => new Pass(file)
        }
    }

}

object Parse {

    /**
     * Converts an input raw dataframe to a new dataframe with a specified user schema.
     * If the data is not in a proper format, it will return `null`
     *
     * @param df input raw dataframe with all columns as string
     * @param description new fields types to convert
     * @return a new dataframe with the new schema
     */
    protected[execution] def applySchema(df: DataFrame, description: TableDescription): DataFrame = {
        description.fields.foldLeft(df) { (temp, field) =>
            temp.withColumn(field.fieldName, field.colExpression)
        }
    }
}
