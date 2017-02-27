import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Part A
  * Create a list of user IDs, along with the number of distinct songs each user has played.
  */
object A{

  def apply(played: RDD[Played]) : DataFrame = {
    val user_author_song = fiterOutDate(played)
    aggregateByUser(user_author_song)
  }

  private def fiterOutDate(raw: RDD[Played]) : RDD[UserAuthorSong] =
    raw.map(r => UserAuthorSong(r.userId, r.author, r.song))
      .distinct()

  private def aggregateByUser(user_author_song:RDD[UserAuthorSong]): DataFrame = {
    val sql = new SQLContext(user_author_song.sparkContext)
    import sql.implicits._
    user_author_song
      .toDF()
      .registerTempTable("user_author_song")

    sql.sql(
      """
          select userId, count(*)
          from user_author_song
          group by userId
          order by userId""")
  }

  case class UserAuthorSong(userId:String, author:String, song:String)
}
