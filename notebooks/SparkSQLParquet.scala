
object Cells {
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.rdd.RDD

  /* ... new cell ... */

  val sqlContext = new SQLContext(sc)
  sqlContext.setConf("spark.sql.shuffle.partitions", "4")  // or set in the notebook metadata; see SparkDataFrames.snb

  /* ... new cell ... */

  import sqlContext.implicits._ 

  /* ... new cell ... */

  import com.lightbend.training.data.Verse

  /* ... new cell ... */

  def v(df:org.apache.spark.sql.DataFrame)=DataFrameWidget.table(df, 5)

  /* ... new cell ... */

  val inputFile = sys.env("TRAINING_HOME")+"/data/kjvdat.txt"

  /* ... new cell ... */

  val versesRDD = for {
    line  <- sc.textFile(inputFile)
    verse <- Verse.parse(line) // If None is returned, this line discards it.
  } yield verse

  /* ... new cell ... */

  val verses = sqlContext.createDataFrame(versesRDD)

  /* ... new cell ... */

  val babylon = verses.filter($"text".contains("Babylon"))

  /* ... new cell ... */

  v(babylon)

  /* ... new cell ... */

  val outPath = sys.env("TRAINING_HOME")+"/output/verses.parquet"

  /* ... new cell ... */

  com.lightbend.training.util.FileUtil.rmrf(new java.io.File(outPath))

  /* ... new cell ... */

  sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
  verses.write.parquet(outPath)

  /* ... new cell ... */

  val verses2 = sqlContext.read.parquet(outPath)

  /* ... new cell ... */

  verses2.printSchema

  /* ... new cell ... */

  val babylon2 = verses2.filter($"text".contains("Babylon"))

  /* ... new cell ... */

  v(babylon2)
}
                  