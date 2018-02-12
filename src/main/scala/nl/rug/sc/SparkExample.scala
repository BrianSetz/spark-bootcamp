package nl.rug.sc

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

case class Person(id: Int, name: String, grade: Double) // For the advanced data set example, has to be defined outside the scope

class SparkExample(sparkSession: SparkSession, pathToCsv: String) {
  private val sparkContext = sparkSession.sparkContext

  /**
    * An example using RDD's, try to avoid RDD's
    */
  def rddExample(): Unit = {
    val data = List(1, 2, 3, 4, 5)
    val rdd = sparkContext.parallelize(data)

    rdd
      .map(x => x + 1) // Increase all numbers by one (apply the transformation x => x + 1 to every item in the set)
      .collect() // Collect the data (send all data to the driver)
      .foreach(println) // Print each item in the list

    printContinueMessage()
  }

  /**
    * An example using Data Frames, improvement over RDD but Data Sets are preferred
    */
  def dataFrameExample(): Unit = {
    import sparkSession.implicits._ // Data in dataframes must be encoded (serialized), these implicits give support for primitive types and Case Classes
    import scala.collection.JavaConverters._

    val schema = StructType(List(
      StructField("number", IntegerType, true)
    ))

    val dataRow = List(1, 2, 3, 4, 5)
      .map(Row(_))
      .asJava

    val dataFrame = sparkSession.createDataFrame(dataRow, schema)

    dataFrame
      .select("number")
      .map(row => row.getAs[Int]("number")) // Dataframe only has the concept of Row, we need to extract the column "number" and convert it to an Int
      .map(_ + 1) // Different way of writing x => x + 1
      .collect()
      .foreach(println)

    dataFrame.printSchema() // Data frames and data sets have schemas

    printContinueMessage()
  }

  /**
    * An example using Data Sets, improvement over both RDD and Data Frames
    */
  def dataSetExample(): Unit = {
    import sparkSession.implicits._ // Data in datasets must be encoded (serialized), these implicits give support for primitive types and Case Classes

    val dataSet = sparkSession.createDataset(List(1, 2, 3, 4, 5))

    dataSet
      .map(_ + 1) // Different way of writing x => x + 1
      .collect()
      .foreach(println)

    dataSet.printSchema()

    printContinueMessage()
  }

  /**
    * Advanced data set example using Scala's Case Classes for more complex data, note that the Case Class is defined at the top of this file
    * We use a CSV containing 3200 US cities as an example data set.
    */
  def dataSetAdvancedExample(): Unit = {
    import sparkSession.implicits._

    val dataSet = sparkSession.createDataset(List(
      Person(1, "Alice", 5.5),
      Person(2, "Bob", 8.6),
      Person(3, "Eve", 10.0)
    ))

    dataSet
      .show() // Shows the table

    printContinueMessage()

    dataSet
      .map(person => person.grade) // We transform the Person(int, string, double) to a double (Person => Double), extracting the person's grade
      .collect() // Collect in case you want to do something with the data
      .foreach(println)

    printContinueMessage()

    // Even cleaner is
    dataSet
      .select("grade")
      .show()

    dataSet.printSchema()

    printContinueMessage()
  }

  /**
    * In your case, you will be reading your data from a database, or (csv, json) file, instead of creating the data in code as we have previously done.
    */
  def dataSetRealisticExample(): Unit = {
    val dataSet = sparkSession.read
      .option("header", "true") // First line in the csv is the header, will be used to name columns
      .option("inferSchema", "true") // Infers the data types (primitives), otherwise the schema will consists of Strings only
      .csv(pathToCsv) // Loads the data from the resources folder in src/main/resources, can also be a path on your storage device

    dataSet.show() // Show first 20 results

    printContinueMessage()

    dataSet
      .sort("name") // Sort alphabetically
      .show()

    printContinueMessage()

    import sparkSession.implicits._ // For the $-sign notation

    dataSet
      .filter($"pop" < 10000) // Filter on column 'pop' such that only cities with less than 10k population remain
      .sort($"lat") // Sort remaining cities by latitude, can also use $-sign notation here
      .show()

    printContinueMessage()

    dataSet
      .withColumn("name_first_letter", $"name".substr(0,1)) // Create a new column which contains the first letter of the name of the city
      .groupBy($"name_first_letter") // Group all items based on the first letter
      .count() // Count the occurrences per group
      .sort($"name_first_letter") // Sort alphabetically
      .show(26) // Returns the number of cities in the US for each letter of the alphabet, shows the first 26 results

    printContinueMessage()

    import org.apache.spark.sql.functions._ // For the round  (...) functionality

    dataSet
      .withColumn("pop_10k", (round($"pop" / 10000) * 10000).cast(IntegerType)) // Create a column which rounds the population to the nearest 10k
      .groupBy("pop_10k") // Group by the rounded population
      .count() // Count the occurences
      .sort("pop_10k") // Sort from low to high
      .show(100) // Returns the number of cities in 10k intervals

    dataSet.printSchema()

    printContinueMessage()
  }

  private def printContinueMessage(): Unit = {
    println("Check your Spark web UI at http://localhost:4040 and press Enter to continue. [Press Backspace and Enter again if pressing enter once does not work]")
    scala.io.StdIn.readLine()
    println("Continuing, please wait....")
  }
}
