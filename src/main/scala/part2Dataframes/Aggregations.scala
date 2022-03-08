package part2Dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.min

object Aggregations extends App {
  val spark = SparkSession
    .builder()
    .appName("Columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.select(countDistinct("Major_Genre")).show()

  moviesDF.select(min("IMDB_Rating")).show()

  moviesDF
    .groupBy("Major_Genre")
    .agg(count("Title").as("Number of movies"))
    .show()

  moviesDF
    .groupBy("Major_Genre")
    .agg(avg("IMDB_Rating"))
    .orderBy(avg("IMDB_Rating"))
    .show()

  moviesDF
    .groupBy("Director")
    .agg(avg("IMDB_Rating"), avg("US_Gross"))
    .orderBy(avg("IMDB_Rating").desc_nulls_last)
    .show()
}
