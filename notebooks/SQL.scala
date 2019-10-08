
object Cells {
  import com.lightbend.training.util.sql.SparkSQLRDDUtil
  import org.apache.spark.sql.SQLContext

  /* ... new cell ... */

  val sqlContext = new SQLContext(sparkContext)
  import sqlContext.implicits._ 

  /* ... new cell ... */

  val datadir = s"${sys.env("TRAINING_HOME")}/data/"
  val flightsPath   = s"${datadir}airline-flights/alaska-airlines/2008.csv"
  val carriersPath  = s"${datadir}airline-flights/carriers.csv"
  val airportsPath  = s"${datadir}airline-flights/airports.csv"
  val planesPath    = s"${datadir}airline-flights/plane-data.csv"

  /* ... new cell ... */

  val (flightsRDD, carriersRDD, airportsRDD, planesRDD) =
    SparkSQLRDDUtil.load(sc, flightsPath, carriersPath, airportsPath, planesPath)

  /* ... new cell ... */

  // Create and register corresponding DataFrames as temporary "tables":
  val flights  = sqlContext.createDataFrame(flightsRDD)
  flights.cache
  flights.registerTempTable(flightsRDD.name)

  /* ... new cell ... */

  def v(df:org.apache.spark.sql.DataFrame) = DataFrameWidget.table(df, 25)

  /* ... new cell ... */

  println(s"DataFrame: ${flightsRDD.name}")
  flights.printSchema()
  println("\nCall show() to print 20 (default) records, truncated width:")
  flights.show()  
  println("\nCall show(5) to print 5, truncated width:")
  flights.show(5)
  println("\nCall show(5) to print 5, with truncate = false:")
  flights.show(5, truncate = false)

  /* ... new cell ... */

  def v(df:org.apache.spark.sql.DataFrame) = DataFrameWidget.table(df, 25)

  /* ... new cell ... */

  v(flights)

  /* ... new cell ... */

  val carriers = sqlContext.createDataFrame(carriersRDD)
  carriers.cache
  carriers.registerTempTable(carriersRDD.name)
  println(s"DataFrame: ${carriersRDD.name}")
  carriers.printSchema()
  v(carriers)

  /* ... new cell ... */

  val airports = sqlContext.createDataFrame(airportsRDD)
  airports.cache
  airports.registerTempTable(airportsRDD.name)
  println(s"DataFrame: ${airportsRDD.name}")
  airports.printSchema()
  v(airports)

  /* ... new cell ... */

  val planes = sqlContext.createDataFrame(planesRDD)
  planes.cache
  planes.registerTempTable(planesRDD.name)
  println(s"DataFrame: ${planesRDD.name}")
  planes.printSchema()
  v(planes)

  /* ... new cell ... */

  <p><span>"flights schema"</span> <code>{flights.schema.treeString}</code></p>

  /* ... new cell ... */

  :markdown 
  total_flights: **${flights.count}**

  /* ... new cell ... */

  import sqlContext.sql

  /* ... new cell ... */

  // How many flights were cancelled (American spelling allows "canceled")?
  v(sql("SELECT COUNT(*) FROM flights f WHERE f.canceled > 0"))

  /* ... new cell ... */

  val c = 0
  v(sql(s"""
    SELECT f.date.month AS month, COUNT(*) as cnt
    FROM flights f
    WHERE f.canceled != $c
    GROUP BY f.date.month
    ORDER BY cnt DESC, month"""))

  /* ... new cell ... */

  v(sql("""
    SELECT origin, dest, COUNT(*) AS cnt
    FROM flights
    GROUP BY origin, dest
    ORDER BY cnt DESC, origin, dest"""))

  /* ... new cell ... */

  v(sql("""
    SELECT
    MIN(times.actualElapsedTime) AS min,
    MAX(times.actualElapsedTime) AS max,
    AVG(times.actualElapsedTime) AS avg,
    SUM(times.actualElapsedTime) AS sum
    FROM flights"""))

  /* ... new cell ... */

  v(sql("SELECT COUNT(*) AS count FROM flights"))

  /* ... new cell ... */

  // None are NULL, so the count will be the same
  v(sql("SELECT COUNT(tailNum) AS count FROM flights"))

  /* ... new cell ... */

  v(sql("SELECT COUNT(DISTINCT tailNum) AS countd FROM flights"))

  /* ... new cell ... */

  v(sql("SELECT APPROXIMATE COUNT(DISTINCT tailNum) AS countd FROM flights"))

  /* ... new cell ... */

  v(sql("""
    SELECT f.*, p.*
    FROM flights f JOIN planes p ON f.tailNum = p.tailNum"""))
}
                  