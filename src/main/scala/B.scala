import org.apache.spark.rdd.RDD

/**
  * Part B
  * Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times
  * each was played.
  */
object B{
  def apply(raw:RDD[Played]) : Array[Result] =
    raw
      .map (p => ((p.author, p.song), 1L))
      .reduceByKey(_+_)
      .top(100)(Ordering.by{case (k,count) => count })
      .map{case ((artist, song), count) => Result(artist, song, count )}

  case class Result(artist: String, song: String , played: Long)

}
