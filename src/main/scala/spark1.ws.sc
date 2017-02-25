import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrameReader, SQLContext}

implicit class RichRead(read: DataFrameReader){
  def csv(file: String, header: Boolean = false, delimiter:String="\t") =
    read
      .format("com.databricks.spark.csv")
      .option("header", header.toString) // Use first line of all files as header
      .option("quote", "\000")
      .option("delimiter", delimiter)
      .load(file)

}
val conf = new SparkConf().setAppName("X").setMaster("local[4]")
val sc = new SparkContext(conf)

val sql = new SQLContext(sc)
import sql.implicits._
import sc._

println("Reading CSV")
case class Played(user:String, author: String, song: String)
sql
  .read.csv("lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv")
.sample(true, 0.0001)
.write.parquet("sample")
