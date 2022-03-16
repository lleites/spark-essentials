package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.regexp_extract

object CommonTypes extends App {
  val spark = SparkSession
    .builder()
    .appName("CommonTypes")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDf = spark.read.json("src/main/resources/data/cars.json")

  def getCarNames(): List[String] = List("chevrolet", "ford")

  val filteredCarsDF =
    carsDf.where(
      regexp_extract(col("Name"), getCarNames().mkString("|"), 0) =!= ""
    )
  filteredCarsDF.show
  println(filteredCarsDF.count())

  val containsFilter = getCarNames.map(col("Name").contains(_)).reduce(_ or _)

  val filteredCarsDFContains =
    carsDf.where(
      containsFilter
    )
  filteredCarsDFContains.show
  println(filteredCarsDFContains.count())

}
