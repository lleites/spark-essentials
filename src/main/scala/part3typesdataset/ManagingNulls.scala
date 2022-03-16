package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ManagingNulls extends App {
  val spark = SparkSession
    .builder()
    .appName("ManagingNulls")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  moviesDF
    .select(
      col("Title"),
      coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10),
      col("Rotten_Tomatoes_Rating").as("R"),
      col("IMDB_Rating") * 10
    )
    .show()

  moviesDF.where(col("IMDB_Rating").isNotNull).show()

  moviesDF.select("Title", "IMDB_Rating").na.drop().show()
  moviesDF
    .select("Title", "Rotten_Tomatoes_Rating", "IMDB_Rating")
    .na
    .fill(0, List("Rotten_Tomatoes_Rating", "IMDB_Rating"))
    .show()

}
