import org.scalatest.{FreeSpec, Matchers}

class ATest extends FreeSpec with Matchers with TestFixtures{

  "single row" in {
    runA(Array(user1_song1)) should be (
      Array(("user-1", 1))
    )
  }

  "ignore the same song" in {
    val input = Array(
      user1_song1
      ,user1_song1
      ,user1_song1
      ,user1_song1
    )

    runA(input) should be (
      Array(("user-1", 1))
    )
  }


  "works with multiple users" in {
    val input = Array(
      user1_song1
      ,user1_song2
      ,user2_song3
      ,user2_song4
    )

    runA(input) should be (Array(
      ("user-1", 2),
      ("user-2", 2)
    ))
  }

  private def runA(input: Array[Played]) = spark.withLocalSQLContext{ sql =>
    A(sql.sparkContext.parallelize(input).coalesce(1))
      .collect()
  }


}

