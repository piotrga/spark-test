import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object B{
  def apply(raw:RDD[Played]): DataFrame = {
    val sql = new SQLContext(raw.sparkContext)
    import sql.implicits._

    raw.toDF()
      .registerTempTable("played")

    sql.sql("select author, song, count(*) from played group by author, song order by count(*) desc limit 100")
  }
}
