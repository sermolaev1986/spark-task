package spark.task

import net.liftweb.json.DefaultFormats
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTask {

  case class ColumnConfiguration(existing_col_name: String, new_col_name: String, new_data_type: String)

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Provide parameters in this order: dataPath, delimiter, columnConfigs")

    val dataPath = args(0)
    val delimiter = args(1)
    val columnConfigs = args(2)

    val jsonString =
      """
        |[
        |{"existing_col_name" : "name", "new_col_name" : "first_name", "new_data_type" : "string"},
        |{"existing_col_name" : "age", "new_col_name" : "total_years", "new_data_type" : "integer"}
        |]
        |""".stripMargin


    val sparkSession = SparkSession
      .builder
      .appName("spark-task")
      .getOrCreate()

    println("\n-------Step 1 load to data frame------\n")

    var df = sparkSession.read
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("quote", "'")
      .option("inferSchema", "true")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .csv(dataPath)

    df.printSchema()
    df.show(10)

    println("\n-------Step 2 remove rows with empty values------\n")
    df = filterOutRowsWithEmptyStrings(df)
    df.show(10)

    println("\n-------Step 3 convert columns------\n")
    val columnConfigurations = parseColumnConfigurations(jsonString)
    df = applyColumnChanges(columnConfigurations, df)
    df.printSchema()
    df.show(10)

    sparkSession.stop()
  }

  def parseColumnConfigurations(rawConfigurationString: String): List[ColumnConfiguration] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val columnConfigs = net.liftweb.json.parse(rawConfigurationString)
    columnConfigs.children.map(_.extract[ColumnConfiguration])
  }

  def applyColumnChanges(columnConfigurations: List[ColumnConfiguration], df: DataFrame): DataFrame = {
    var varDF = df

    for (columnConfig <- columnConfigurations) {
      varDF = varDF
        .withColumnRenamed(columnConfig.existing_col_name, columnConfig.new_col_name)
        .withColumn(columnConfig.new_col_name, col(columnConfig.new_col_name).cast(columnConfig.new_data_type))
    }

    varDF
  }

  def filterOutRowsWithEmptyStrings(df: DataFrame): DataFrame = {
    val stringColumns = df.schema
      .filter(field => field.dataType == StringType)
      .map(_.name)

    var varDF = df
    for (stringColumn <- stringColumns) {
      varDF = varDF.filter(stringColumn + " is null OR " + "trim(" + stringColumn + ") != ''")
    }

    varDF
  }
}
