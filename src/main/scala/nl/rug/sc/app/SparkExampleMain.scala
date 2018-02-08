package nl.rug.sc.app

import nl.rug.sc.SparkExample

object SparkExampleMain extends App {
  val example = new SparkExample()

  // NOTE: change the log level in the log4j.properties found in src/main/resources/log4j.properties. For demo purpose
  // the log level is WARN, showing only warnings, but for development INFO is recommended, for even more details use
  // DEBUG or TRACE as the log level.
  // https://logging.apache.org/log4j/2.x/manual/customloglevels.html

  example.rddExample()
  example.dataFrameExample()
  example.dataSetExample()
  example.dataSetAdvancedExample()
  example.dataSetRealisticExample()

  println("Done")
}
