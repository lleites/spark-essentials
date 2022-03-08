package part2Dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

object ColumnsAndExpressions extends App {
  val spark = SparkSession
    .builder()
    .appName("Columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  moviesDF.select("Title", "Release_Date").show()

  val movesDFProfit = moviesDF.withColumn(
    "Total_Profit",
    col("US_Gross") + col(
      "Worldwide_Gross"
    )
  )

  movesDFProfit.select("Total_Profit").show()

  moviesDF
    .filter(col("Major_Genre") === "Comedy")
    .filter(col("IMDB_Rating") > 6)
    .select("Title", "Major_Genre", "IMDB_Rating")
    .show()

}
