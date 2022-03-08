package part2Dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.max

object Joins extends App {

  def readSqlDF(name: String) = spark.read
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

  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val salaries = readSqlDF("salaries")

  salaries.show()

  val employees = readSqlDF("employees")

  employees.show()

  val maxSalaries =
    salaries.groupBy("emp_no").agg(max("salary").as("max_salary"))

  val employeesMaxSalaries = employees
    .join(maxSalaries, "emp_no")
    .select("first_name", "last_name", "max_salary")
    .sort(col("max_salary").desc)

  employeesMaxSalaries.show()

  val managers = readSqlDF("dept_manager")

  val notManagers = employees
    .join(managers, Seq("emp_no"), "left_anti")
    .select("first_name", "last_name")

  println(employees.count())
  println(notManagers.count())
  notManagers.show()

  val titles = readSqlDF("titles")
  val mostRecentTitle = titles.groupBy("emp_no", "title").agg(max("to_date"))


  val top10Titles = maxSalaries
    .sort(col("max_salary").desc)
    .limit(10)
    .join(mostRecentTitle, "emp_no")

  val columnsToSelect = maxSalaries.columns :+ "title"
  top10Titles.select(columnsToSelect.toSeq.map(col): _*).show()

}
