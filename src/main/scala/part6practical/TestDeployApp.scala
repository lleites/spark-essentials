package part6practical

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SaveMode

object TestDeployApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      print("we need inout and output path")
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("Test deploy App").getOrCreate()
    val moviesDf = spark.read.json(args(0))

    val goodComediesDF = moviesDf
      .select(
        col("Title"),
        col("IMDB_Rating").as("Rating"),
        col("Release_Date").as("Release")
      )
      .where(col("Major_Genre") === "Comedy" and col("Rating") > 6.5)
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show()

    goodComediesDF.write.mode(SaveMode.Overwrite).json(args(1))
  }
}
