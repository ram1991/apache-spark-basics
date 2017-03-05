package basics.spark

import common.spark.InitSpark
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

import scala.reflect.ClassTag

object Correlations extends InitSpark {
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

    val toyStoryRaters = movieRatings
                         .filter(mr=>mr.movie==1)
                         .map(mr=>mr.userId)
                         .collect()
                         .toSet
    val ratedByToyStoryers = movieRatings.filter(mr=>toyStoryRaters.contains(mr.userId) && !(mr.movie==1))
    ratedByToyStoryers.groupBy("movie")
    .count()
    .withColumn("percent_rated",col = col("count") / toyStoryRaters.size)
    .sort(desc("percent_rated"))
    .show(4)

    val columns = moviesData.columns
    var mvd = moviesData.drop(columns(0),columns(1))

    val movieNamesIterator = movies.iterator
    for(col<-mvd.columns) {
      mvd = mvd.withColumnRenamed(col,movieNamesIterator.next()._1.toString)
    }
    val correlations = columnPairCorrelations(mvd)
    correlations.filter(cor=>cor.item1=="1" || cor.item2=="1").sort(desc("corr")).show(4)
    close
  }

  def columnPairCorrelations(ds:DataFrame):Dataset[ItemCorrelation] = {
    import spark.implicits._
    val columns = ds.columns
    val noOfColumns = columns.length
    var correlations = Vector[ItemCorrelation]()
    for(i<-0 until noOfColumns) {
      for(j<-(i+1) until noOfColumns) {
        var twoCols = ds.select(columns(i),columns(j)).rdd.collect().unzip[Any,Any](r=>(r(0),r(1)), ClassTag.Any,ClassTag.Any)
        twoCols = twoCols._1.zip(twoCols._2).filter(e=>e._1!=null && e._2!=null).unzip
        val firstColumn = sc.parallelize(twoCols._1.filter(e=>e!=null).map(_.toString).map(_.toDouble))

        val secondColumn = sc.parallelize(twoCols._2.filter(e=>e!=null).map(_.toString).map(_.toDouble))
//        val corr = ds.stat.corr(columns(i),columns(j))
        val corr = Statistics.corr(firstColumn,secondColumn)
        val itemCorrelation = ItemCorrelation(columns(i),columns(j),corr)

        correlations = correlations :+ itemCorrelation
      }
    }
    spark.createDataset(correlations)
  }
}
