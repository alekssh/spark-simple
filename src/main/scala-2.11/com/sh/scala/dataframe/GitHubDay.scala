package com.sh.scala.dataframe

import org.apache.spark.sql.SparkSession

import scala.io.Source


/**
  * @author Alexander Shulga
  */
object GitHubDay {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Git hub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputPath = "src/main/resources/github-archive/*.json"

    val ghLog = spark.read.json(inputPath)

    val pushes = ghLog.filter("type = 'PushEvent'")

    pushes.cache()

    pushes.printSchema()
    System.out.println("all events: " + ghLog.count())
    System.out.println("push events: " + pushes.count())
    pushes.show(5)

    val grouped = pushes.groupBy("actor.login").count()
    grouped.show(5)

    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    import spark.implicits._
    val empPath = "src/main/resources/ghEmployees.txt"
    val employees = Set() ++ (for {line <- Source.fromFile(empPath).getLines()} yield line.trim)

    val bcEmployees = sc.broadcast(employees)

    val isEmp = (user: String) => bcEmployees.value.contains(user)
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)

    val filtered = ordered.filter(isEmployee($"login"))

    filtered.show()
  }


}
