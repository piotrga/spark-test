import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SaveMode}

trait SparkSugars {

  implicit class RichRead(read: DataFrameReader) {
    def csv(file: String, header: Boolean = false, delimiter: String = "\t") : DataFrame =
      read
        .format("com.databricks.spark.csv")
        .option("header", header.toString)
        .option("quote", "\000") // ignore quotes
        .option("delimiter", delimiter)
        .load(file)

  }

  implicit class RichWrite(writer: DataFrameWriter) {
    def csv(file: String, header: Boolean = false, delimiter: String = "\t"): Unit =
      writer
        .format("com.databricks.spark.csv")
        .option("header", header.toString)
        .option("delimiter", delimiter)
        .mode(SaveMode.Overwrite)
        .save(file)
  }



}
