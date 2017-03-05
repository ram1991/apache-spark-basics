package basics.spark

final case class MovieRating(userId: Int, gender: Int, movie: Int,movieName:String, rating: Int,
                             isRatingPresent: Int, fourAndAbove:Int)
final case class ItemCorrelation(item1:String,item2:String,corr:Double)
