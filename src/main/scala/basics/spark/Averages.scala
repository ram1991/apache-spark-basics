package basics.spark

import common.spark.InitSpark
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._


object Averages extends InitSpark {
  def main(args: Array[String]) = {

    var moviesData = readerWithoutHeader.csv("HW1-data.csv")
    val headers = moviesData.first()
    val movies = headers.toSeq.drop(2).map(_.toString).map(_.split(":")).map(arr=>(arr(0).toInt,arr(1)))
    val headerless = moviesData.rdd
                     .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    moviesData = spark.createDataFrame(headerless, moviesData.schema)

    implicit val encoder = Encoders.product[MovieRating]
    val movieRatings = moviesData.flatMap(r => {
      val userId = r.getString(0).toInt
      val gender = r.getString(1).toInt
      val sz = r.size
      val data = Array.ofDim[MovieRating](sz - 2)
      for (i <- 2 until sz) {
        val movie = movies(i-2)._1
        val movieName = movies(i-2)._2
        val rating = Option(r.getString(i)).map(_.toInt).getOrElse(0)
        val isRatingPresent = if (rating == 0) 0 else 1
        val isRatingFourAndAbove = if(rating>= 4) 1 else 0
        val movieRating = MovieRating(userId, gender, movie = movie, movieName, rating, isRatingPresent, isRatingFourAndAbove)
        data(i - 2) = movieRating
      }
      data
    })

    val agg = movieRatings.groupBy("movie")
              .sum("isRatingPresent", "rating","fourAndAbove")
              .withColumnRenamed("sum(isRatingPresent)", "number_of_ratings")
              .withColumnRenamed("sum(rating)", "total")
              .withColumnRenamed("sum(fourAndAbove)","num_four_and_above")
              .withColumn("avg_rating", col("total") / col("number_of_ratings"))
              .withColumn("over_four_percent",col("num_four_and_above")*100 / col("number_of_ratings"))
    agg.sort(desc("number_of_ratings")).show(3)
    agg.sort(desc("avg_rating")).show(3)
    agg.sort(desc("over_four_percent")).show(3)

    close
  }
}
