package nl.rug.sc.app

import nl.rug.sc.app.SparkLocalMain.run
import org.apache.spark.SparkFiles
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.SparkSession

object SparkSubmitMain extends App with SparkBootcamp {
  // How to Run:
  //
  // * You must first download the Spark 2.2.1 archive (spark-2.2.1-bin-hadoop2.7.tgz) and extract it. Then, set the environment variable SPARK_HOME to the path of the extracted directory
  // e.g. SPARK_HOME=/home/<youruser>/Downloads/spark-2.2.1-bin-hadoop2.7/
  //
  // * Create a Spark cluster, using Docker:
  // > docker run -d --rm --name spark-master -p 4040:4040 -p 8080-8081:8080-8081 -p 7077:7077 briansetz/docker-spark:2.2.1 spark/sbin/start-master.sh
  // > MASTER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark-master)
  // > docker run -d --rm --name spark-slave briansetz/docker-spark:2.2.1 spark/sbin/start-slave.sh spark://${MASTER_IP}:7077
  //
  // * Create a .JAR file that is to be submitted to Spark using the sbt-assembly plugin (provided in this project)
  // Run this command (in the root of this project) to create the jar:
  // > sbt assembly
  //
  // * CD to the $SPARK_HOME/bin directory and run the following command:
  // > ./spark-submit --class nl.rug.sc.app.SparkSubmitMain --deploy-mode client --master spark://localhost:7077 <your-path-to>/spark-bootcamp/target/scala-2.11/spark-bootcamp-assembly-0.1.jar

  // IMPORTANT: running this command will actually fail the 'dataSetRealisticExample' example, other examples will complete. It will not be able to find the CSV file. The reason for this is that we are running on a remote Spark.
  // We use client mode, which means that the driver is running on this machine, but the computations are performed on the Spark workers. Since the driver is running on this machine,
  // the line: getClass.getResource("/csv/2014_us_cities.csv").getPath, will evaluate to some local path on this machine, which the remote Spark workers cannot access (since the path is local to your machine.
  // The way to address this issue is to host the data in a way that it can be accessed remotely, for your project you will have to use a distributed filesystem (HDFS) or a (no)SQL database.
  //
  // Also note that even if we switch from --deploy-mode client to --deploy-mode cluster, which means the driver is actually running on the Spark master and not on your machine, it will still fail.
  // In this case, the path will resolve to some path on Spark master, which the Spark slaves cannot access, similar to the situation where they could not access the paths on your local machine.
  // (Actually, the driver will not run on the Spark master, but the Spark master finds a slave to run the driver on)
  //
  // Sometimes it is critical to reduce network latency, which means submitting using --deploy-mode cluster, since the driver will be running on the Spark slaves.
  // Especially if you have to return large quantities of data to the driver, running it on your machine (--deploy-mode client) can be very slow. However,
  // the client deployment mode does allow for command line interaction (since the driver is running locally). If you want to deploy with cluster mode, you have to
  // copy the (jar) file(s) to the cluster itself.

  // Note: when using spark-submit we do not define a master, the master definition is passed to the spark-submit command
  override def sparkSession = SparkSession // Usually you only create one Spark Session in your application, but for demo purpose we recreate them
    .builder()
    .appName("spark-bootcamp")
    .getOrCreate()

  override def pathToCsv = getClass.getResource("/csv/2014_us_cities.csv").getPath

  run() // Run is defined in the tait SparkBootcamp
}
