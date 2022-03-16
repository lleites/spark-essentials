package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession
    .builder()
    .appName("ComplexTypes")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  moviesDF
    .select(
      col("Title"),
      to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"),
      col("Release_Date")
    )
    .withColumn("Today", current_date())
    .withColumn(
      "Movie_Age",
      datediff(col("Today"), col("Actual_Release")) / 365
    )
    .show()

  val stocksDF =
    spark.read
      .option("header", true)
      .csv("src/main/resources/data/stocks.csv")

  stocksDF.show

  stocksDF.withColumn("ParsedDate", to_date(col("date"), "MMM d YYYY")).show()

  moviesDF.printSchema()

  val moviesDFStruct =
    moviesDF.withColumn("Profit", struct("Us_Gross", "Worldwide_Gross"))
  moviesDFStruct.printSchema()

  moviesDFStruct
    .select(col("Profit").getField("Us_Gross").as("P_Us_Gross"))
    .show()

  val containsIgnoreCase = udf((x: Seq[String], y: String) =>
    x.map(_.toLowerCase()).contains(y.toLowerCase())
  )
  moviesDF
    .select(col("Title"), split(col("Title"), " |,").as("Words"))
    .select(
      col("Words"),
      size(col("Words")),
      array_contains(col("Words"), "love"),
      col("Words").getItem(0),
      containsIgnoreCase(col("Words"), lit("love"))
    )
    .show(50)

}
