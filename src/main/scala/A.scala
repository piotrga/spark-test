import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Part A
  * Create a list of user IDs, along with the number of distinct songs each user has played.
  */
object A{

  def apply(played: RDD[Played]) : RDD[(String, Long)] = {
    played
      .map(r => UserSong(r.userId, r.songId ))
      .distinct()
      .map(x => (x.userId, 1L))
      .reduceByKey(_+_)

  }

  case class UserSong(userId:String, songId:String)

}
