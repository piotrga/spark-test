//import java.io.FileWriter
//
//import org.apache.spark
//import org.apache.spark._
//import org.apache.spark.sql._
//
//import scala.util.Random
//
//object X extends App{
//  val FILE= "/tmp/small.csv"
//  val ROWS= 100000
//  val SEP = '\n'
////  save()
//
//  def save(): Unit = {
//    val it = Iterator.fill(ROWS)(List.fill(600)(Random.nextInt(20)*1000000000L))
//    println("Saving...")
//    val headers = (1 to 600).map(i => "col-" + i)
//
//    val fw = new FileWriter(FILE)
//    fw.write(headers.mkString(",")+SEP)
//    try it.foreach(row => fw.write(row.mkString(",")+SEP)) finally fw.close()
//    println("Saving - DONE")
//  }
//
//
//  val conf = new SparkConf().setAppName("X").setMaster("local[4]")
//  val sc = new SparkContext(conf)
//
//  val sql = new SQLContext(sc)
//  import sql.implicits._
//
//
//  println("Reading CSV")
//
//  val x = sql
//    .read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") // Use first line of all files as header
//    .option("maxColumns", 1000)
//    .option("maxCharsPerColumn", 10000)
//    .option("inferSchema", "true") // Automatically infer data types
//    .load(FILE)
//
//  println("Saving...")
//
//  x
//    .coalesce(1)
//    .write.parquet("/tmp/small.parquet")
//  println("Saving  - DONE")
//}
