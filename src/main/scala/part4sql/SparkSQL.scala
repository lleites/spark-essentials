package part4sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
object SparkSQL extends App {
  val spark = SparkSession
    .builder()
    .appName("SparkSQL")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDf = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/cars.json")

  carsDf.createOrReplaceTempView("cars")
  val americansCars = spark.sql(
    """
    select Name from cars where Origin = 'USA'
    """.stripMargin
  )

  americansCars.show()

  spark
    .sql(
      """
    show databases
    """.stripMargin
    )
    .show

  def readTable(name: String): DataFrame = spark.read
    .format("jdbc")
    .options(
      Map(
        "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
        "dbtable" -> s"public.${name}",
        "user" -> "docker",
        "password" -> "docker"
      )
    )
    .load()

  def transferTables(tableNames: Seq[String]) = tableNames.foreach {
    tableName =>
      val tableDf = readTable(tableName)
      tableDf.createOrReplaceTempView(tableName)
      tableDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

  transferTables(
    Seq(
      "employees",
      "departments",
      "dept_emp",
      "dept_manager",
      "movies",
      "salaries",
      "titles"
    )
  )

  val employeesTable = spark.read.table("employees")
  employeesTable.show()

}
