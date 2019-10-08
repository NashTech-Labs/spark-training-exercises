
object Cells {
  import com.lightbend.training.util.sql.SparkSQLRDDUtil
  import com.lightbend.training.data._

  /* ... new cell ... */

  import org.apache.spark.sql.SQLContext
  val sqlContext = new SQLContext(sparkContext)
  import sqlContext.implicits._ 
  import org.apache.spark.sql.functions._  // for min, max, etc. column operations
  import sqlContext.sql                    // Makes it easier to write SQL queries.

  /* ... new cell ... */

  val home = sys.env("TRAINING_HOME")
  val datadir = s"$home/data"
  val airdir = s"$datadir/airline-flights"
  val flightsPath  = airdir + "/alaska-airlines/2008.csv"
  val carriersPath = airdir + "/carriers.csv"
  val airportsPath = airdir + "/airports.csv"
  val planesPath   = airdir + "/plane-data.csv"
  
  val (flightsRDD, carriersRDD, airportsRDD, planesRDD) =
    SparkSQLRDDUtil.load(sc, flightsPath, carriersPath, airportsPath, planesPath)

  /* ... new cell ... */

  val flights  = sqlContext.createDataFrame(flightsRDD)
  flights.cache
  flights.registerTempTable(flightsRDD.name)
  println(s"DataFrame: ${flightsRDD.name}")
  flights.printSchema()

  /* ... new cell ... */

  def v(df:org.apache.spark.sql.DataFrame) = DataFrameWidget.table(df, 25)

  /* ... new cell ... */

  v(flights)

  /* ... new cell ... */

  val flightsDS = flightsRDD.toDS()
  flightsDS.cache
  // No method: flightsDS.registerTempTable(flightsRDD.name)
  println(s"Dataset: ${flightsRDD.name}")
  flightsDS.printSchema()
  flightsDS.show(5)

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

  val c2 = carriers.toDF("abbreviation", "name")
  println("Old schema")
  carriers.printSchema
  println("New schema")
  c2.printSchema

  /* ... new cell ... */

  sql("SELECT COUNT(*) FROM flights").show()
  println(s"total_flights = ${flights.count}")

  /* ... new cell ... */

  v(sql("SELECT * FROM flights f WHERE f.canceled > 0"))

  /* ... new cell ... */

  val canceled_flights = flights.filter(flights("canceled") > 0)
  v(canceled_flights)

  /* ... new cell ... */

  canceled_flights.cache
  ()

  /* ... new cell ... */

  canceled_flights.explain(extended = false)

  /* ... new cell ... */

  canceled_flights.explain(extended = true)

  /* ... new cell ... */

  v(sql("SELECT * FROM flights ORDER BY origin LIMIT 2"))

  /* ... new cell ... */

  v(flights.orderBy(flights("origin"))) ++
  // flights.orderBy("origin")   // Won't run these two; they return the same results.
  // flights.orderBy($"origin")  // same
  v(flights.orderBy($"origin".desc)) ++   // Descending sort
  // flights.orderBy(col("origin").desc)       // Won't show output, to reduce "noise".
  v(flights.orderBy(flights("origin"), flights("dest")))  // Two or more columns (comma separated)

  /* ... new cell ... */

  v(canceled_flights.groupBy("date.month").count().orderBy($"count".desc))

  /* ... new cell ... */

  canceled_flights.unpersist
  ()

  /* ... new cell ... */

  v(sql("""
    SELECT origin, dest, COUNT(*) AS cnt
    FROM flights
    GROUP BY origin, dest
    ORDER BY cnt DESC, origin, dest"""))

  /* ... new cell ... */

  val flights_between_airports = flights.select($"origin", $"dest").
    groupBy($"origin", $"dest").count().
    orderBy($"count".desc, $"origin", $"dest")
  flights_between_airports.cache
  v(flights)

  /* ... new cell ... */

  v(flights.groupBy($"origin", $"dest").count().coalesce(2).   // "Coalesce" down to 2 partitions
    sort($"count".desc, $"origin", $"dest"))

  /* ... new cell ... */

  v(sql("""
     SELECT
       MIN(times.actualElapsedTime) AS min,
       MAX(times.actualElapsedTime) AS max,
       AVG(times.actualElapsedTime) AS avg,
       SUM(times.actualElapsedTime) AS sum
   FROM flights"""))

  /* ... new cell ... */

  v(flights.agg(
    min($"times.actualElapsedTime"),
    max($"times.actualElapsedTime"),
    avg($"times.actualElapsedTime"),
    sum($"times.actualElapsedTime")))

  /* ... new cell ... */

  widgets.column(
    v(sql("SELECT COUNT(*) AS count FROM flights")),
    v(sql("SELECT COUNT(tailNum) AS count FROM flights")),
    v(sql("SELECT COUNT(DISTINCT tailNum) AS countd FROM flights")),
    v(sql("SELECT APPROXIMATE COUNT(DISTINCT tailNum) AS countad FROM flights")),
    v(flights.agg(count($"*"))),         // i.e. COUNT(*)
    v(flights.agg(count($"tailNum"))),   // same; no tailNum = NULL
    v(flights.agg(countDistinct($"tailNum"))),
    v(flights.agg(approxCountDistinct($"tailNum")))
  )

  /* ... new cell ... */

  v(sql("SELECT f.*, p.* FROM flights f JOIN planes p ON f.tailNum = p.tailNum"))

  /* ... new cell ... */

  v(flights.join(planes, flights("tailNum") === planes("tailNum")))
}
                  