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

  def loadTriplets(sparkSession: SparkSession, path: String): RDD[Triple] = {
    val tripletsRDD = sparkSession.sparkContext.textFile(path)
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(line =>
        RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
    tripletsRDD
  }
  def getSemanticGraph(tripleRDD: RDD[Triple]): org.apache.spark.graphx.Graph[Node, Node] = {
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
}

class TripleReader(config: DQAssessmentConfiguration, sparkSession: SparkSession, period: String) extends ReaderRDF{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    val graph = load(sparkSession, config.hdfsInputPath + "sample.nt")

    graph.vertices.collect().foreach(println(_))
    graph.edges.collect().foreach(println(_))

  }
}
