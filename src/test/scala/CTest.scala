import C.Session
import org.scalatest.{FreeSpec, GivenWhenThen, Matchers}

class CTest extends FreeSpec with Matchers with TestFixtures with GivenWhenThen{

  "single row" in {
    runB(Array(user1_song1)) should be (
      Array(("user-1", Session(1239152267000L, 1239152267000L, List("Song-1"))))
    )
  }

  "multiple rows" in {
    import scala.concurrent.duration._
    val input = Array(
      user1_song1,
      user1_song1.delay(10.minutes),
      user1_song1.delay(20.minutes)
    )

    runB(input) should be (
      Array(("user-1", Session(1239152267000L, 1239152267000L + 20.minutes.toMillis, List("Song-1", "Song-1", "Song-1"))))
    )
  }

  "multiple sessions for the same user" in {
    import scala.concurrent.duration._
    val input = Array(
      // session 1
      user1_song1,
      user1_song1.delay(10.minutes),
      user1_song1.delay(20.minutes),
      //session 2
      user1_song1.delay(1.hour)
    )

    runB(input) should have length 2
  }

  "returns 10 longest" in {
    import scala.concurrent.duration._
    val _11 = (1 to 11).toArray
      .flatMap (i => (1 to i) map ( x => user1_song1.delay(x.minutes).copy(userId = i.toString, songId = s"song-$i-$x") ) )

    val result = runB(_11)
    result should have length 10
    result.map(_._2.size) should be ((2 to 11).toList.reverse)
  }

  private def runB(input: Array[Played]) : Array[(String, C.Session)] = spark.withLocalSQLContext{ sql =>
    C(sql.sparkContext.parallelize(input).coalesce(1))
  }

}

