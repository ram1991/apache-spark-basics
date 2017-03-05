package content_filtering.spark

import basics.spark.MovieRating
import common.spark.InitSpark
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

case class TagData(movieId:Int,userId:Int,tag:String,timestamp:Long)
case class IDFData(tag:String,idf_one_plus:Double,idf:Double)

object ContentFiltering extends InitSpark {
  def main(args: Array[String]) = {

    import spark.implicits._
    val tagData = reader.csv("tags.csv").as[TagData]

    val nm = tagData.agg(countDistinct("movieId")).collect().map(r=>r.getLong(0))
    val numRated = nm(0).toDouble
    val logNumRated = Math.log(numRated)
    val numDocs = 2500.0
    val logNumDocs = Math.log(numDocs)
    val countUsedLog = logNumDocs

    val df = tagData.groupBy("tag").count()
    val idf = df.withColumn("one_plus_count",col("count")+1)
    .withColumn("log_count_one_plus",log(col("one_plus_count")))
    .withColumn("idf_one_plus",negate(col("log_count_one_plus")) + countUsedLog)
    .withColumn("log_count",log(col("count")))
    .withColumn("idf",negate(col("log_count")) + countUsedLog)
    .drop("count","one_plus_count","log_count_one_plus","log_count").as[IDFData]

    val idfs = idf.collect()
    val idfMap = idfs.map(i=>(i.tag,i.idf)).toMap
    val idfMapOnePlus = idfs.map(i=>(i.tag,i.idf_one_plus)).toMap

    val tgArr = tagData.collect()
    val tf = tagData.groupBy("movieId","tag").count().withColumnRenamed("count","tf")

    val tfAndIdf = tf.join(idf,"tag")
                   .withColumn("tf_idf",col("tf") * col("idf"))
                   .withColumn("tf_idf_one_plus",col("tf") * col("idf_one_plus"))
                   .withColumn("tf_idf_sq",pow("tf_idf",2.0))
                   .withColumn("tf_idf_one_plus_sq",pow("tf_idf_one_plus",2.0))
    val norms = tfAndIdf.groupBy("movieId").sum("tf_idf_sq","tf_idf_one_plus_sq")
                .withColumnRenamed("sum(tf_idf_sq)","sum")
                .withColumnRenamed("sum(tf_idf_one_plus_sq)","sum_one_plus")
                .withColumn("norm",sqrt("sum"))
                .withColumn("norm_one_plus",sqrt("sum_one_plus"))

    val tfIdf = tfAndIdf.join(norms,"movieId")
    .withColumn("tf_idf_norm",col("tf_idf") / col("norm"))
    .withColumn("tf_idf_norm_one_plus",col("tf_idf_one_plus") / col("norm_one_plus"))
                .drop("tf","idf","idf_one_plus","tf_idf","tf_idf_one_plus","tf_idf_sq","tf_idf_one_plus_sq","sum","sum_one_plus")

    tfIdf.show(5)

    tfIdf.where("movieId=2231").show(15)



    close
  }
}
