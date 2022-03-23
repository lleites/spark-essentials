package part7bigdata

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaxiApplication extends App {
  val spark = SparkSession
    .builder()
    .appName("Columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val taxiDF =
    spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()
  //println(taxiDF.count())

  val taxisZoneDF = spark.read
    .option("header", true)
    .csv("src/main/resources/data/taxi_zones.csv")
  taxisZoneDF.printSchema()
  //taxisZoneDF.show()

  def groupByTaxi(joinColName: String, groupBy: Seq[String]): DataFrame =
    taxiDF
      .join(
        taxisZoneDF,
        taxiDF.col(joinColName) === taxisZoneDF
          .col("LocationId")
      )
      .groupBy(groupBy.map(col): _*)
      .agg(count("Zone").as("totalTrips"))
      .orderBy(col("totalTrips").desc_nulls_last)

  val pickupsByTaxiZoneDF =
    groupByTaxi("PULocationID", Seq("PULocationID", "Zone", "Borough"))
  //pickupsByTaxiZoneDF.show()

  val pickupsByTaxiBoroughDF = groupByTaxi("PULocationID", Seq("Borough"))
  //pickupsByTaxiBoroughDF.show()

  val pickupsByHourDF = taxiDF
    .withColumn("hour_of_day", hours(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count(col("hour_of_day")).as("totalCount"))
    .orderBy(col("totalCount").desc_nulls_last)
  //pickupsByHourDF.show()

  val tripsDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30

  val tripsWithLengthDF = tripsDistanceDF.withColumn(
    "is_long",
    col("distance") >= longDistanceThreshold
  )

  val tripByLengthDF = tripsWithLengthDF.groupBy(col("is_long")).count()
  //tripByLengthDF.show()

  val pickupDropoffPopularityDF = taxiDF
    .withColumn(
      "is_long",
      col("trip_distance") >= longDistanceThreshold
    )
    .where(not(col("is_long")))
    .groupBy("PULocationID", "DOLocationId")
    .agg(count("*").as("TotalTrips"))
    .join(
      taxisZoneDF,
      col("PULocationID") === col("LocationId")
    )
    .withColumnRenamed("Zone", "PUZone")
    .drop("LocationId")
    .join(
      taxisZoneDF,
      col("DOLocationId") === col("LocationId")
    )
    .drop("LocationId")
    .withColumnRenamed("Zone", "DOZone")
    .orderBy(col("TotalTrips").desc_nulls_last)
    .select("PUZone", "DOZone", "TotalTrips")

  //pickupDropoffPopularityDF.show()

  val ratecodeEvolution = taxiDF
    .groupBy(
      to_date(col("tpep_pickup_datetime")).as("pickup_day"),
      col("RatecodeID")
    )
    .agg(count("*").as("total_trips"))
    .orderBy("pickup_day")

  ratecodeEvolution.show()

}
