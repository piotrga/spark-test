import org.scalatest.{FreeSpec, GivenWhenThen, Matchers}

class BTest extends FreeSpec with Matchers with TestFixtures with GivenWhenThen{

  "single row" in {
    runB(Array(user1_song1)) should be (
      Array(Result("Rolling Stones", "Let's rock", 1))
    )
  }

  "counts" in {
    val input = Array(
      user1_song1
      ,user1_song1
      ,user1_song1
      ,user1_song1
    )

    runB(input) should be (
      Array(Result("Rolling Stones", "Let's rock", 4))
    )
  }

  "returns top 100" in {
    Given("110 songs where song x was played x times")
    val _110 = (1 to 110).toList flatMap (i => List.fill(i)(user1_song1.copy(song = "song-"+i) ))

    When("Running transformation B")
    val res = runB(_110.toArray)
    res should have length 100

    Then("It returns songs played 110..11 times")
    res.map(_.played) should be((11 to 110).reverse.toList)
  }

  private def runB(input: Array[Played]) = spark.withLocalSQLContext{ sql =>
    B(sql.sparkContext.parallelize(input).coalesce(1))
      .collect()
      .map(r => Result(r.getString(0), r.getString(1), r.getLong(2) ))

  }

  case class Result(artist: String, song: String , played: Long)

}

