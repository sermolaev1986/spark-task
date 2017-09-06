package spark.task

import net.liftweb.json.{DefaultFormats, Extraction, JsonAST, Printer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTask {

  case class ColumnConfiguration(existing_col_name: String, new_col_name: String, new_data_type: String)

  case class ColumnProfileInfo(column: String, unique_values: Int, values: Map[Any, Any])

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Provide parameters in this order: dataPath, delimiter, columnConfigs")

    val dataPath = args(0)
    val delimiter = args(1)
    val columnConfigs = args(2)

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
    val columnConfigurations = parseColumnConfigurations(columnConfigs)
    df = applyColumnChanges(columnConfigurations, df)
    df.printSchema()
    df.show(10)

    println("\n-------Step 4 profile columns------\n")
    val profileResult = profileColumns(df)
    implicit val formats = DefaultFormats + MapSerializer
    val profileResultAsJson = Printer.pretty(JsonAST.render(Extraction.decompose(profileResult)))
    println(profileResultAsJson)

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

    val columnsToRemove = df.schema.fieldNames.diff(columnConfigurations.map(_.existing_col_name))
    varDF = varDF.drop(columnsToRemove: _*)

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

  def profileColumns(df: DataFrame): Array[ColumnProfileInfo] = {
    df.rdd
      .flatMap(row =>
        row.schema.fieldNames.zipWithIndex.map { case (fieldName, index) => ((fieldName, row(index)), 1L) }
      )
      .reduceByKey(_ + _)
      .filter { case ((fieldName, fieldValue), count) => fieldValue != null }
      .map { case ((fieldName, fieldValue), count) => (fieldName, (fieldValue, count))}
      .groupByKey()
      .collect()
      .map { case (fieldName, fieldValues) => ColumnProfileInfo(fieldName, fieldValues.size, fieldValues.toMap) }
  }

  import net.liftweb.json._
  object MapSerializer extends Serializer[Map[Any, Any]] {
    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case m: Map[_, _] => JObject(m.map({
        case (k, v) => JField(
          k match {
            case ks: String => ks
            case ks: Symbol => ks.name
            case ks: Any => ks.toString
          },
          Extraction.decompose(v)
        )
      }).toList)
    }

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), Map[Any, Any]] = {
      sys.error("Not interested.")
    }
  }
}