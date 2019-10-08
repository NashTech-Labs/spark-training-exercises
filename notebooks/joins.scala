
object Cells {
  import com.lightbend.training.data._
  import com.lightbend.training.util.FileUtil
  import scala.io.Source
  
  val home = sys.env("TRAINING_HOME")
  val datadir = s"$home/data/"
  val outdir = s"$home/output/"

  /* ... new cell ... */

  val inputPath = datadir + "airline-flights/alaska-airlines/2008.csv"

  /* ... new cell ... */

  val flights = for {
    line <- sparkContext.textFile(inputPath)
    flight <- Flight.parse(line)
    if flight.date.month == 1
  } yield (flight.origin -> flight)

  /* ... new cell ... */

  flights.take(5)  // look at a few examples. Click the table icon to see them.

  /* ... new cell ... */

  val airportsPath = datadir + "airline-flights/airports.csv"
  val airports = for {
    line <- sparkContext.textFile(airportsPath)
    airport <- Airport.parse(line)
  } yield (airport.iata -> airport.airport)

  /* ... new cell ... */

  flights.cache
  airports.cache

  /* ... new cell ... */

  airports.take(5)  // look at a few examples. Click the table icon to see them.

  /* ... new cell ... */

  val flights_airports = flights.join(airports)

  /* ... new cell ... */

  flights_airports.toDebugString

  /* ... new cell ... */

  if (flights.count != flights_airports.count) {
    s"flights count, ${flights.count}, doesn't match output count, ${flights_airports.count}"
  } else { 
    s"counts are matching: ${flights.count}"
  }

  /* ... new cell ... */

  val flights_airports2 = flights_airports map {
    case (_, tup) => tup
  }

  /* ... new cell ... */

  val outpath = outdir + "airline-flights-airports-rdd-join"
  s"Writing output to: $outpath"
  FileUtil.rmrf(new java.io.File(outpath))
  flights_airports2.saveAsTextFile(outpath)

  /* ... new cell ... */

  Source.fromFile(outpath+"/part-00000").getLines.take(20).foreach(println)
}
                  