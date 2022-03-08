package part2Dataframes

import org.apache.spark.sql.SparkSession

import java.util.Properties

object WriteDB extends App {
  val spark = SparkSession
    .builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.show()

  moviesDF.write
    .format("jdbc")
    .options(
      Map(
        "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
        "dbtable" -> "public.movies",
        "user" -> "docker",
        "password" -> "docker"
      )
    )
    .save()

}
