package nl.rug.sc.app

import nl.rug.sc.SparkExample
import org.apache.spark.sql.SparkSession

object SparkLocalMain extends App with SparkBootcamp {
  private val master = "local[*]" // local[*] means a local cluster with * being the amount of workers, * = 1 worker per cpu core. Always have at least 2 workers (local[2])

  override def sparkSession = SparkSession // Usually you only create one Spark Session in your application, but for demo purpose we recreate them
    .builder()
    .appName("spark-bootcamp")
    .master(master)
    .getOrCreate()

  override def pathToCsv = getClass.getResource("/csv/2014_us_cities.csv").getPath

  run() // Run is defined in the tait SparkBootcamp
}
