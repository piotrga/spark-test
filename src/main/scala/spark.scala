import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object spark{
  def localSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setAppName("X")
      .setMaster("local[4]")
      .set("spark.ui.enabled", "false")
    new SparkContext(conf)
  }

  def withLocalSQLContext[T](f : SQLContext => T) = {
    val sc = spark.localSparkContext()
    val sql = new SQLContext(sc)
    try f(sql) finally sc.stop()
  }

}
