package org.uclm.alarcos.rrc.io
import java.io.{ByteArrayInputStream, File}

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.graphx._
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.apache.jena.graph._
import org.apache.jena.riot.{Lang, RDFDataMgr}
//import net.sansa_stack.rdf.spark.io.NTripleReader
//import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness.InterlinkingCompleteness._
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import scala.util.Random
/**
  * Created by raulreguillo on 6/09/17.
  */

trait ReaderRDF extends Serializable{
  protected val log: Logger = LogManager.getLogger(this.getClass.getName)
  protected val processSparkSession: SparkSession

  def execute()

//  def readTripletsAlt(path: String): Unit = {
//    val input = processSparkSession.sparkContext.textFile(path)
//
//    val triplesRDD = NTripleReader.load(processSparkSession, path)
//
//    triplesRDD.collect().foreach(println(_))
//    println(assertTriples(triplesRDD))
//  }

  def load(session: SparkSession, path: String): org.apache.spark.graphx.Graph[Node, Node] = {
    getSemanticGraph(loadTriplets(session, path))
  }

  def loadTriplets(session: SparkSession, path: String): RDD[Triple] = {
    val tripleRDD = session.sparkContext.textFile(path)
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(line =>
        RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
    //subjects.collect().foreach(println(_))
    tripleRDD
  }
  def getSemanticGraph(tripleRDD: RDD[Triple]): org.apache.spark.graphx.Graph[Node, Node] = {
    val extTripleRDD = tripleRDD.map(triple => (triple, triple.getSubject().hashCode().toLong, triple.getObject().hashCode().toLong))

    val subjects: RDD[(VertexId, Node)] = tripleRDD.map(triple => (triple.getSubject().hashCode().toLong, triple.getSubject()))
    val predicates: RDD[Edge[Node]] = extTripleRDD.map(line => Edge(line._2, line._3, line._1.getPredicate()))
    val objects: RDD[(VertexId, Node)] = tripleRDD.map(triple => (triple.getObject().hashCode().toLong, triple.getObject()))

    val graph = org.apache.spark.graphx.Graph(subjects.union(objects), predicates)
    //println("Num vert: " + graph.numVertices + "\nNum edges: " + graph.numEdges)
    graph
  }
  def readTriplets(path: String): Unit = {
    import processSparkSession.implicits._
    log.info(s"Load DataFrame from Triplets $path")
    val ran = new Random()
    val triplets = processSparkSession.read.textFile(path).map(line => line.split(" "))
    val subs = triplets.map(line => (ran.nextLong(), line(0)))
    val preds = triplets.map(line => line(1))
    val objs = triplets.map(line => (ran.nextLong(), line(2)))

    triplets.show(10, truncate = false)
    //val userGraph: Graph[String, String] = Graph(subjects.union(objects), predicates, "")

    val subjects: RDD[(VertexId, String)] = subs.union(objs).rdd
    val predicates: RDD[Edge[String]] = preds.map(line => Edge(ran.nextLong(), ran.nextLong(),line)).rdd

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
    val graph = org.apache.spark.graphx.Graph(users, relationships, defaultUser)

    val res = graph.edges.mapValues(edge => edge.srcId)

    res.collect().map(line => print(line))
    graph.vertices.collect().map(vert => print(vert))

  }
  def showTripletsRDD(tripletsRDD: RDD[Triple]): Unit = {
    tripletsRDD.collect().foreach(println(_))
  }
}

class TripleReader(config: DQAssessmentConfiguration, sparkSession: SparkSession, period: String) extends ReaderRDF{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    //val df = readTriplets(config.hdfsInputPath + "*.nt")
    val df = load(sparkSession, config.hdfsInputPath + "*.nt")
    println(df.toString())
    //df.collect().map(line => println(line.getSubject().toString() + " :: " + line.getPredicate() + " :: " + line.getObject()))

  }
}
