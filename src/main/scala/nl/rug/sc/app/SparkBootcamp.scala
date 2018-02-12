package nl.rug.sc.app

import nl.rug.sc.SparkExample
import nl.rug.sc.app.SparkLocalMain.sparkSession
import org.apache.spark.sql.SparkSession

trait SparkBootcamp { // A trait can be compared to a Java Interface
  def sparkSession: SparkSession // this def has to be implemented when extending this trait
  def pathToCsv: String // path to csv file, also has to be implemented

  def run(): Unit = {
    val example = new SparkExample(sparkSession, pathToCsv)

    // NOTE: change the log level in the log4j.properties found in src/main/resources/log4j.properties. For demo purpose
    // the log level is WARN, showing only warnings, but for development INFO is recommended, for even more details use
    // DEBUG or TRACE as the log level.
    // https://logging.apache.org/log4j/2.x/manual/customloglevels.html

    example.rddExample()
    example.dataFrameExample()
    example.dataSetExample()
    example.dataSetAdvancedExample()
    example.dataSetRealisticExample()

    sparkSession.stop()
    println("Done")
  }
}
