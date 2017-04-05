package com.sh.scala.dataframe

import org.apache.spark.sql.SparkSession

/**
  * Created by alex on 4/5/17.
  */
object PostDataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Posts processing")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val posts = sc.textFile("src/main/resources/italianPosts.csv").map(_.split("~"))
    posts.cache()

    /*
        val postsDfTuple = DataFrameCreator.viaTuple(spark, posts)
        val postsDfCase = DataFrameCreator.viaCase(spark, posts)
    */
    val postsDf = DataFrameCreator.viaStruct(spark, posts)

    postsDf.printSchema()
    postsDf.show(3)

    val idAndBody = postsDf.select("id", "body")
    idAndBody.show(3)

    postsDf.select($"body" contains "Italiano").show(3)

    val questions = postsDf.where($"postTypeId" === 1)

    questions.withColumn("ratio", 'viewCount / 'score).where('ratio < 35).show()

    questions.sort('lastActivityDate desc).show(10)

    import org.apache.spark.sql.functions._

    println(questions.withColumn("activePeriod", datediff('lastActivityDate, 'creationDate))
      .orderBy('activePeriod desc).head.getString(3).replace("&lt;", "<").replace("&gt;", ">"))
  }

}




