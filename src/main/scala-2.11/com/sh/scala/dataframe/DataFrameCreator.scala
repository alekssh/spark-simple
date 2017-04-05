package com.sh.scala.dataframe


import com.sh.scala.dataframe.StringImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by alex on 4/5/17.
  */

object DataFrameCreator {

  def viaTuple(spark: SparkSession, posts: RDD[Array[String]]): DataFrame = {

    import spark.implicits._
    val postsTuple = posts.map(l => (l(0), l(1), l(2), l(3), l(4), l(5), l(6), l(7), l(8), l(9), l(10), l(11), l(12)))

    postsTuple.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate",
      "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")

  }

  def viaCase(spark: SparkSession, posts: RDD[Array[String]]): DataFrame = {
    import spark.implicits._
    posts.map(r =>
      Post(r(0).toIntSafe,
        r(1).toTimestampSafe,
        r(2).toLongSafe,
        r(3),
        r(4).toIntSafe,
        r(5).toTimestampSafe,
        r(6).toIntSafe,
        r(7),
        r(8),
        r(9).toIntSafe,
        r(10).toLongSafe,
        r(11).toLongSafe,
        r(12).toLong)
    ).toDF()

  }

  def viaStruct(spark: SparkSession, posts: RDD[Array[String]]): DataFrame = {
    val postSchema = StructType(Seq(
      StructField("commentCount", IntegerType, true),
      StructField("lastActivityDate", TimestampType, true),
      StructField("ownerUserId", LongType, true),
      StructField("body", StringType, true),
      StructField("score", IntegerType, true),
      StructField("creationDate", TimestampType, true),
      StructField("viewCount", IntegerType, true),
      StructField("title", StringType, true),
      StructField("tags", StringType, true),
      StructField("answerCount", IntegerType, true),
      StructField("acceptedAnswerId", LongType, true),
      StructField("postTypeId", LongType, true),
      StructField("id", LongType, false))
    )

    val rowRdd = posts.map(r =>
      Row(r(0).toIntSafe.getOrElse(null),
        r(1).toTimestampSafe.getOrElse(null),
        r(2).toLongSafe.getOrElse(null),
        r(3),
        r(4).toIntSafe.getOrElse(null),
        r(5).toTimestampSafe.getOrElse(null),
        r(6).toIntSafe.getOrElse(null),
        r(7),
        r(8),
        r(9).toIntSafe.getOrElse(null),
        r(10).toLongSafe.getOrElse(null),
        r(11).toLongSafe.getOrElse(null),
        r(12).toLong)
    )

    spark.createDataFrame(rowRdd, postSchema)
  }

}

case class Post(commentCount: Option[Int],
                lastActivityDate: Option[java.sql.Timestamp],
                ownerUserId: Option[Long],
                body: String,
                score: Option[Int],
                creationDate: Option[java.sql.Timestamp],
                viewCount: Option[Int],
                title: String,
                tags: String,
                answerCount: Option[Int],
                acceptedAnswerId: Option[Long],
                postTypeId: Option[Long],
                id: Long)




