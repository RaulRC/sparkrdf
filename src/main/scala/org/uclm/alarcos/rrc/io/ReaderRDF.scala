package org.uclm.alarcos.rrc.io

import java.io.{ByteArrayInputStream}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql._
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.apache.jena.graph._
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by raulreguillo on 6/09/17.
  */
trait ReaderRDF extends Serializable{
  protected val log: Logger = LogManager.getLogger(this.getClass.getName)
  protected val processSparkSession: SparkSession

  def execute()

  def loadGraph(session: SparkSession, path: String): org.apache.spark.graphx.Graph[Node, Node] = {
    getSemanticGraph(loadTriplets(session, path))
  }

  def loadSubjects(session: SparkSession, path: String): RDD[Node] = {
    loadTriplets(session, path)
      .map(triplet => triplet.getSubject())
      .distinct()
  }
  def loadPredicates(session: SparkSession, path: String): RDD[Node] = {
    loadTriplets(session, path)
      .map(triplet => triplet.getPredicate())
      .distinct()
  }
  def loadObjects(session: SparkSession, path: String): RDD[Node] = {
    loadTriplets(session, path)
      .map(triplet => triplet.getObject())
      .distinct()
  }
  def loadTriplets(sparkSession: SparkSession, path: String): RDD[Triple] = {
    val tripletsRDD = sparkSession.sparkContext.textFile(path)
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(line =>
        RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
    tripletsRDD
  }
  private def getSemanticGraph(tripleRDD: RDD[Triple]): org.apache.spark.graphx.Graph[Node, Node] = {
    //Generate hashCodes for graphx representation
    val extTripleRDD = tripleRDD.map(triple => (triple, triple.getSubject().hashCode().toLong, triple.getObject().hashCode().toLong))
    val subjects: RDD[(VertexId, Node)] = tripleRDD.map(triple => (triple.getSubject().hashCode().toLong, triple.getSubject()))
    val predicates: RDD[Edge[Node]] = extTripleRDD.map(line => Edge(line._2, line._3, line._1.getPredicate()))
    val objects: RDD[(VertexId, Node)] = tripleRDD.map(triple => (triple.getObject().hashCode().toLong, triple.getObject()))
    val graph = org.apache.spark.graphx.Graph(subjects.union(objects).distinct(), predicates.distinct())
    graph
  }

  def showTripletsRDD(tripletsRDD: RDD[Triple]): Unit = {
    tripletsRDD.collect().foreach(println(_))
  }

  //RDF Operations
  def getSubjectsWithProperty(graph: org.apache.spark.graphx.Graph[Node, Node], property: String): VertexRDD[Node] = {
    val objectPropertyId = graph.vertices.filter(vert => vert._2.hasURI(property)).first()._1
    val vertexSubjectIds = graph.edges.filter(line => line.dstId == objectPropertyId).map(line => line.srcId)
    val subjectVertices = graph.vertices
      .join(vertexSubjectIds.keyBy((i => i)))
      .map(line => (line._1, line._2._1))
    val result = org.apache.spark.graphx.VertexRDD(subjectVertices)
    result
  }

}

class TripleReader(config: DQAssessmentConfiguration, sparkSession: SparkSession, inputFile: String) extends ReaderRDF{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    //val graph = loadGraph(sparkSession, config.hdfsInputPath + "*.nt")
    val graph = loadGraph(sparkSession, inputFile)
    graph.vertices.collect().foreach(println(_))
//    graph.edges.collect()foreach(println(_))
    val subjectVertices = getSubjectsWithProperty(graph, "http://xmlns.com/foaf/0.1/Person")
    subjectVertices.collect().foreach(println(_))

    //graph.edges.filter( line => line.attr.getURI().equals  )
    //graph.connectedComponents().vertices.collect().foreach(println(_))
  }
}