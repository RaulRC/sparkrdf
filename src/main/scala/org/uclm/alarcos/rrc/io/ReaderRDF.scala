package org.uclm.alarcos.rrc.io
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.graphx._
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.apache.spark.rdd.RDD
/**
  * Created by raulreguillo on 6/09/17.
  */

trait ReaderRDF extends Serializable{
  protected val log: Logger = LogManager.getLogger(this.getClass.getName)
  protected val processSparkSession: SparkSession

  def execute()

    def readTriplets(path: String): Unit = {
    import processSparkSession.implicits._
    log.info(s"Load DataFrame from Triplets $path")
    val triplets = processSparkSession.read.textFile(path).map(line => line.split(" "))
    val subjects = triplets.map(line => line(0))
    val predicates = triplets.map(line => line(1))
    val objects = triplets.map(line => line(2))

    triplets.show(10, truncate = false)
    //val userGraph: Graph[String, String] = Graph(subjects.union(objects), predicates, "")
    val users: RDD[(VertexId, (String, String))] =
      processSparkSession.sparkContext.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    processSparkSession.sparkContext.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

  }
}

class TripleReader(config: DQAssessmentConfiguration, sparkSession: SparkSession, period: String) extends ReaderRDF{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val df = readTriplets(config.hdfsInputPath + "*.nt")
  }
}
