object WriteSample extends App with SparkSugars {



  writeSample("lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv", "sample.csv")

  private def writeSample(in:String, out:String) = spark.withLocalSQLContext{
    _
      .read.csv(in)
      .sample(true, 0.00001)
      .coalesce(1)
      .write.csv(out)
  }

}
