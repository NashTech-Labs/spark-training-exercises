
object Cells {
  import com.lightbend.training.util.FileUtil
  import scala.io.Source
  
  val home = sys.env("TRAINING_HOME")
  val datadir = s"$home/data/"
  val outdir = s"$home/output"

  /* ... new cell ... */

  import org.apache.spark.rdd.RDD

  /* ... new cell ... */

  val ints: RDD[Int] = sc.parallelize(0 until 5)

  /* ... new cell ... */

  ints.name = "ints"

  /* ... new cell ... */

  val tupleVects: RDD[Vector[(Int,Int)]] = ints.map {
    i => (0 until 6).map(j => (i,j)).to[Vector]
  }
  tupleVects.name = "tupleVects"

  /* ... new cell ... */

  tupleVects.dependencies map (_.rdd.name)

  /* ... new cell ... */

  tupleVects foreach println

  /* ... new cell ... */

  val sums = tupleVects map { vect =>
    vect.foldLeft((0,0)) {
      case ((currentIndex, sum), (i, j)) => (i, sum + (i * 100) + j)
    }
  }
  sums.name = "sums"

  /* ... new cell ... */

  val sumDeps1 = sums.dependencies
  println(s"sums.dependencies before checkpointing: # = ${sumDeps1.size}")
  println(s"The dependencies names: ${sumDeps1 map (_.rdd.name)}")

  /* ... new cell ... */

  val checkpointDir = s"$outdir/checkpoints"
  
  sc.setCheckpointDir(checkpointDir)
  sums.checkpoint

  /* ... new cell ... */

  println(s"sums is checkpointed? ${sums.isCheckpointed}")
  println(s"sums' checkpoint file: ${sums.getCheckpointFile}")

  /* ... new cell ... */

  val cdir = new java.io.File(checkpointDir)
  println(s"Exists? ${cdir.exists}")
  println(s"Is a Directory? ${cdir.isDirectory}")
  println(s"Contents yet? (I.e., you may not see any output after this message)")
  FileUtil.print(cdir)

  /* ... new cell ... */

  sums.cache
  
  sums foreach println

  /* ... new cell ... */

  val sumDeps2 = sums.dependencies
  println(s"sums.dependencies after checkpointing: # = ${sumDeps2.size}")
  println(s"The dependencies names: ${sumDeps2 map (_.rdd.name)}")

  /* ... new cell ... */

  println("Checkpoint dir should now have at least one checkpoint dir and files:")
  FileUtil.print(cdir)

  /* ... new cell ... */

  val partitions = sums.partitions
  println(s" for ${partitions.size} partitions:")
  partitions foreach { p =>
    println(s"Partition #${p.index}: preferred locations: ${sums.preferredLocations(p)}")
  }

  /* ... new cell ... */

  sums.persist()

  /* ... new cell ... */

  sums.getStorageLevel

  /* ... new cell ... */

  val sums8 = sums.repartition(8)
  println(s"sums8 has ${sums8.partitions.size} partitions")

  /* ... new cell ... */

  sums8.cache
  sums8.count

  /* ... new cell ... */

  val sums2 = sums.coalesce(2)
  println(s"sums2 has ${sums2.partitions.size} partitions")
}
                  