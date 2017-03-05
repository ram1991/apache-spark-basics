package basics.spark

import common.spark.InitSpark
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.sql.functions._

import scala.reflect.ClassTag

object RatingsByGender extends InitSpark {
  def main(args: Array[String]) = {

    import spark.implicits._
    val moviesData = reader.csv("HW1-data.csv")
    val movies = moviesData.columns.toSeq.drop(2).map(_.toString).map(_.split(":")).map(arr=>(arr(0).toInt,arr(1)))


    implicit val encoder = Encoders.product[MovieRating]
    val movieRatings = moviesData.flatMap(r => {
      val userId = r.getInt(0)
      val gender = r.getInt(1)
      val sz = r.size
      var data = List[MovieRating]()
      for (i <- 2 until sz) {
        val movie = movies(i-2)._1
        val movieName = movies(i-2)._2
        val movieRating = if(r.isNullAt(i)) {
          List[MovieRating]()
        } else {
          Option(r.getInt(i)).map(r=>{
            val isRatingFourAndAbove = if(r>= 4) 1 else 0
            val movieRating = MovieRating(userId, gender, movie = movie,movieName, r, 1, isRatingFourAndAbove)
            movieRating
          }).toList
        }
        data = movieRating ++ data
      }
      data
    })

    val groupedByGender = movieRatings.groupBy("gender","movie")
    .agg(avg("rating"),count("userId"),sum("fourAndAbove"))
    .withColumnRenamed("avg(rating)", "avg_rating")
    .withColumnRenamed("count(userId)", "total")
    .withColumnRenamed("sum(fourAndAbove)", "above_four")
    .withColumn("above_four_fraction",col("above_four") / col("total"))



    val maleRatings = groupedByGender.filter(r=>r.getAs("gender")==0)
                      .withColumnRenamed("avg_rating", "male_rating")
                      .withColumnRenamed("above_four_fraction", "male_above_four")
                      .drop("gender","above_four","total")

    val femaleRatings = groupedByGender.filter(r=>r.getAs("gender")==1)
                        .withColumnRenamed("avg_rating", "female_rating")
                        .withColumnRenamed("above_four_fraction", "female_above_four")
                        .drop("gender","above_four","total")

    val combinedRatings = maleRatings
                          .join(femaleRatings,"movie")
                          .withColumn("rating_diff",col("male_rating") - col("female_rating"))
                          .withColumn("above_four_diff",col("male_above_four") - col("female_above_four"))
    combinedRatings.sort(desc("rating_diff")).show(3)
    combinedRatings.sort(asc("rating_diff")).show(3)

    combinedRatings.sort(desc("above_four_diff")).show(3)
    combinedRatings.sort(asc("above_four_diff")).show(3)

    val groupedByOnlyGender = movieRatings.groupBy("gender")
                          .agg(avg("rating"),count("userId"),sum("fourAndAbove"))
                          .withColumnRenamed("avg(rating)", "avg_rating")
                          .withColumnRenamed("count(userId)", "total")
                          .withColumnRenamed("sum(fourAndAbove)", "above_four")
                          .withColumn("above_four_fraction",col("above_four") / col("total"))
    groupedByOnlyGender.show(2)

    close
  }
}
