import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD

import scala.util.Try

object C{
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  case class Song(timestamp:Long, id:String)

  def apply(played:RDD[Played]) : Array[(String, Session)] =
    played
      .flatMap(p => Try(p.userId, Song(df.parse(p.timestamp).getTime, p.songId)).toOption)
      .groupByKey() // big shuffle..., could combine with sorting (repartitionAndSortWithinPartitions) but the solution would be way less elegant. See branch version2
      .mapValues(_.toList.sortBy(_.timestamp)) // O(n log n), should be ok in memory as we're not expecting humongus playlists per user
      .flatMapValues(toSessions) // O(n)
      .top(10)(Ordering.by(_._2.size)) // O(n) + tiny shuffle

  case class Session(firstSongTimestamp:Long, lastSongTimestamp:Long, songsReversed: List[String]) {
    def songs = songsReversed.reverse
    lazy val size = songsReversed.size
  }

  private def toSessions(songs: Iterable[Song]) : List[Session] = {
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
