
object Cells {
  if (sys.env.isDefinedAt("TRAINING_HOME") == false) {
    println("""
      ********************** ERROR **********************
      
      The TRAINING_HOME environment variable must be 
      defined. Please see the course instructions for 
      details. After setting the variable, please restart
      the Spark Notebook app.
      
      ********************** ERROR **********************
    """)
  } else {
    println(s"Using TRAINING_HOME: ${sys.env("TRAINING_HOME")}")
  }

  /* ... new cell ... */

  val datadir = s"${sys.env("TRAINING_HOME")}/data/"

  /* ... new cell ... */

  sparkContext  // The already-defined SparkContext instance. We'll use an alias for it: "sc"

  /* ... new cell ... */

  val input = sc.textFile(s"${datadir}/all-shakespeare.txt")

  /* ... new cell ... */

  def lower(string: String) = string.toLowerCase
  
  // The following return the same results:
  input.map(lower)
  input.map(line => lower(line))

  /* ... new cell ... */

  val lines = input.map(line => line.toLowerCase)

  /* ... new cell ... */

  lines.toDebugString

  /* ... new cell ... */

  val wordCount = lines.flatMap(line => line.split("""\W+"""))
                       .groupBy(word => word)
                       .mapValues(seq => seq.size)
                       .cache()

  /* ... new cell ... */

  println(s"There are ${wordCount.count} unique words")

  /* ... new cell ... */

  s"There are ${wordCount.count} unique words"

  /* ... new cell ... */

  wordCount.take(100).foreach(println)

  /* ... new cell ... */

  wordCount.take(10) foreach println

  /* ... new cell ... */

  wordCount take 10 foreach println
}
                  