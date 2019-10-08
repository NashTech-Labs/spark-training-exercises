
object Cells {
  import com.lightbend.training.util.FileUtil // Used below

  /* ... new cell ... */

  val home = sys.env("TRAINING_HOME")
  val datadir = s"$home/data/"
  val outdir = s"$home/output"

  /* ... new cell ... */

  val input = sc.textFile(s"${datadir}/all-shakespeare.txt")
                .map(_.toLowerCase)

  /* ... new cell ... */

  val wcB = input.flatMap(line => line.split("""\W+"""))
                .countByValue()

  /* ... new cell ... */

  val outpathB = s"${outdir}/shakespeare-wcB.txt" // This path will be treated as a file, not directory now.
  val outpathFileB = new java.io.File(outpathB)
  FileUtil.rmrf(outpathFileB)
  s"Writing ${wcB.size} records to $outpathB"

  /* ... new cell ... */

  val outB = new java.io.PrintWriter(outpathB)
  wcB foreach {
    case (word, count) => outB.println("%20s\t%d".format(word, count))
  }
  
  // WARNING: Without this close statement, it appears the output stream is
  // not completely flushed to disk!
  outB.close()

  /* ... new cell ... */

  FileUtil.print(outpathFileB)
  import scala.io.Source
  Source.fromFile(outpathB).getLines.take(20).foreach(println)

  /* ... new cell ... */

  :sh ls -l ${outpathB}

  /* ... new cell ... */

  :sh head ${outpathB}
}
                  