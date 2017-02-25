import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.uncommons.maths.statistics.DataSet

import scala.concurrent.duration.Duration
import scala.util.Try

case class Played(userId: String, timestamp: String, authorId: String, author:String, songId: String, song: String){
  def delay(d:Duration) = Played.delay(this, d)
}

object Played{
  import scala.concurrent.duration._

  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  def delay(p:Played, duration: Duration) : Played =
    p.copy(timestamp = df.format(new Date(df.parse(p.timestamp).getTime + duration.toMillis)))
}


object AApp extends App with SparkSugars {


  val inputFileName = args(1)
  val outFileName = args(2)

  spark.withLocalSQLContext { sql =>
    import sql.implicits._

    val transform = args(0) match {
      case "A" => A.apply _
      case "B" => B.apply _
      case "C" => (x:RDD[Played]) => sql.createDataFrame(C.apply(x))
    }

    val raw = sql.read.csv(inputFileName)
      .map(r => Played(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5) ))

    transform(raw)
      .coalesce(1)
      .write.csv(outFileName, header = true)
  }
}

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


object B{
  def apply(raw:RDD[Played]) = {
    val sql = new SQLContext(raw.sparkContext)
    import sql.implicits._

    raw.toDF()
      .registerTempTable("played")

    sql.sql("select author, song, count(*) from played group by author, song order by count(*) desc limit 100")
  }
}

object C{
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  case class Song(timestamp:Long, id:String)

  def apply(played:RDD[Played]) : Array[(String, Session)] =
    played
      .flatMap(p => Try(p.userId, Song(df.parse(p.timestamp).getTime, p.songId)).toOption)
      .groupByKey()
      .flatMapValues(findLongestSession)
      .sortBy(_._2.size, ascending = false)
      .take(10)

  case class Session(firstSongTimestamp:Long, lastSongTimestamp:Long, songsReversed: List[String]) {
    def songs = songsReversed.reverse
    lazy val size = songsReversed.size
  }

  def findLongestSession(songs: Iterable[Song]) : List[Session] = {
    import scala.concurrent.duration._
    val initialSession = Session(songs.head.timestamp, songs.head.timestamp, List(songs.head.id))

    songs.tail.foldLeft(List(initialSession)){
      case (lastSession :: sessions, song) if (song.timestamp - lastSession.lastSongTimestamp) <= 20.minutes.toMillis =>
        lastSession.copy(lastSongTimestamp = song.timestamp, songsReversed = song.id :: lastSession.songsReversed ) :: sessions
      case (sessions, song) =>
        Session(song.timestamp, song.timestamp, List(song.id)):: sessions
    }



  }
}


