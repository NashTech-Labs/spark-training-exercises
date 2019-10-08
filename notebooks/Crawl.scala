
object Cells {
  import com.lightbend.training.util.FileUtil
  import scala.io.Source
  
  val home = sys.env("TRAINING_HOME")
  val datadir = s"$home/data/"
  val outdir = s"$home/output"

  /* ... new cell ... */

  val separator = "/"

  /* ... new cell ... */

  val inputPath = datadir + "enron-spam-ham/ham100/*," + datadir + "enron-spam-ham/spam100/*"

  /* ... new cell ... */

  val texts = sparkContext.union(
    inputPath.split(",").toList.map(dir => sparkContext.wholeTextFiles(dir))
  )

  /* ... new cell ... */

  val files_contents = texts.map {
    case (id, text) =>
      val lastSep = id.lastIndexOf(separator)  // Find the last directory separator in the path.
      val id2 = if (lastSep < 0) id.trim else id.substring(lastSep+1, id.length).trim // Remove it and text before it.
      val text2 = text.trim.replaceAll("""\s*\n\s*""", " ") // replace any \n in the contents.
      (id2, text2)
  }

  /* ... new cell ... */

  val outputPath = s"$outdir/crawl-output"
  val outputPathFile = new java.io.File(outputPath)
  FileUtil.rmrf(outputPathFile)
  files_contents.saveAsTextFile(outputPath)

  /* ... new cell ... */

  FileUtil.print(outputPathFile)
  println()
  // Look at some of the contents:
  Source.fromFile(outputPath+"/part-00000").getLines.take(50).foreach(println)
}
                  