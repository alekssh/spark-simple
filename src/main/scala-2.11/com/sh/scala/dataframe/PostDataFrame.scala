package com.sh.scala.dataframe

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by alex on 4/5/17.
  */
object PostDataFrame {

  def main(args: Array[String]): Unit = {

    val resourceDir = args(0)

    val spark = SparkSession.builder()
      .appName("Posts processing")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val posts = sc.textFile(resourceDir + "/italianPosts.csv").map(_.split("~"))
    posts.cache()

    /*
        val postsDfTuple = DataFrameCreator.viaTuple(spark, posts)
        val postsDfCase = DataFrameCreator.viaCase(spark, posts)
    */
    val postsDf = PostDataFrameCreator.viaStruct(spark, posts)

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

    //window per user id
    questions.select('ownerUserId, 'acceptedAnswerId, 'score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser")
      .withColumn("diffToMax", 'maxPerUser - 'score).show(20)


    //user defined function
    val countTags = udf((tags: String) => "&lt;".r.findAllMatchIn(tags).size)
    postsDf.select('tags, countTags('tags) as "tagsCount").show(10, false)


    //drop without accepted answers
    postsDf.na.drop(Array("acceptedAnswerId")).show(10)

    val cleanedTextRdd = postsDf.rdd.map(row => Row.fromSeq(row.toSeq.updated(3, row.getString(3).replace("&lt;", "<").replace("&gt;", ">")).
      updated(8, row.getString(8).replace("&lt;", "<").replace("&gt;", ">"))))

    val cleanPostsDf = spark.createDataFrame(cleanedTextRdd, postsDf.schema)

    cleanPostsDf.show(10)

    //aggregation
    cleanPostsDf.groupBy('ownerUserId).agg(count('id).as("c"), max('lastActivityDate), max('score)).orderBy('c desc) show (5)

    val smplDf = cleanPostsDf.where('ownerUserId >= 13 and 'ownerUserId <= 15)
    smplDf.rollup('ownerUserId, 'tags, 'postTypeId).count.show()
    smplDf.cube('ownerUserId, 'tags, 'postTypeId).count.show()
  }

}




