package part3typesdataset

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.max

import java.util.Date

object Datasets extends App {
  val spark = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val numbersDF =
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/data/numbers.csv")

  numbersDF.show()

  //implicit val intEncoder = Encoders.scalaInt
  import spark.implicits._
  val numbersDS = numbersDF.as[Int]

  numbersDS.printSchema()

  case class Car(
      Name: String,
      Miles_per_Gallon: Option[Double],
      Horsepower: Option[Long],
      Origin: String
  )

  val carsDf = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/cars.json")
  val carsDS = carsDf.as[Car]
  carsDS.show()

  val carNamesDS = carsDS.map(_.Name.toUpperCase())
  carNamesDS.show()

  val carCount = carsDS.count()
  println(carCount)
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carCount)

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  val guitarsDf = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars.json")
    .as[Guitar]

  case class GuitarPlayer(
      id: Long,
      name: String,
      guitars: Seq[Long],
      band: Long
  )
  val guitarPlayersDf = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers.json")
    .as[GuitarPlayer]

  case class Band(id: Long, name: String, hometown: String, year: Long)
  val bandsDf = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands.json")
    .as[Band]

  val guitarPlayersBandDf: Dataset[(GuitarPlayer, Band)] =
    guitarPlayersDf.joinWith(
      bandsDf,
      guitarPlayersDf.col("id") === bandsDf.col("id")
    )

  guitarPlayersBandDf.show

  guitarPlayersDf
    .joinWith(
      guitarsDf,
      array_contains(guitarPlayersDf.col("guitars"), guitarsDf.col("id"))
      //"outer"
    )
    .show
  import org.apache.spark.sql.expressions.scalalang._
  val carsGroupedByOrigin =
    carsDS
      .groupByKey(_.Origin)
      .agg(count("*"), typed.sum[Car](_.Miles_per_Gallon.getOrElse(0L)))
      .show
}
