import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration

object Main extends App with SparkSugars {

  val inputFileName = args(1)
  val outFileName = args(2)

  spark.withLocalSQLContext { sql =>
    import sql.implicits._
    val transform : RDD[Played] => DataFrame  = args(0) match {
      case "A" => x => A(x).toDF("userId", "uniqueSongCount")
      case "B" => x => sql.createDataFrame(B(x))
      case "C" =>  x => sql.createDataFrame(C(x))
    }

    val played = sql.read.csv(inputFileName)
      .map(r => Played(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5) ))

    transform(played)
      .coalesce(1)
      .write.csv(outFileName, header = true)
//      .saveAsTextFile(outFileName)
  }
}

case class Played(userId: String, timestamp: String, authorId: String, author:String, songId: String, song: String){
  def delay(by : Duration) = this.copy(timestamp = Played.df.format(new Date(this.timeststampAsDate.getTime + by.toMillis)))
  def timeststampAsDate : Date = Played.df.parse(timestamp)
}

object Played{
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
}
