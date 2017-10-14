package org.uclm.alarcos.rrc.io

import java.io.{ByteArrayInputStream}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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

  /**
    * Main method for load the graph from a path
    *
    * @param session Spark session
    * @param path Path of the NT files to read. Could be a local, HDFS, S3 path
    * @return Spark GraphX of Nodes
    */
  def loadGraph(session: SparkSession, path: String): org.apache.spark.graphx.Graph[Node, Node] = {
    getSemanticGraph(loadTriplets(session, path))
  }

  /**
    * Returns all the subjects in the file (but not the objects or predicates)
    *
    * @param session Spark session
    * @param path Path of the NT files to read. Could be a local, HDFS, S3 path
    * @return RDD of Nodes which are subjects in the files
    */
  def loadSubjects(session: SparkSession, path: String): RDD[Node] = {
    loadTriplets(session, path)
      .map(triplet => triplet.getSubject())
      .distinct()
  }

  /**
    * Returns all the distinct predicates in the file
    *
    * @param session Spark session
    * @param path Path of the NT files to read. Could be a local, HDFS, S3 path
    * @return RDD of Nodes which are predicates in the files
    */
  def loadPredicates(session: SparkSession, path: String): RDD[Node] = {
    loadTriplets(session, path)
      .map(triplet => triplet.getPredicate())
      .distinct()
  }

  /**
    * Returns all the distinct objects in the file (but not the subjects or predicates)
    *
    * @param session Spark session
    * @param path Path of the NT files to read. Could be a local, HDFS, S3 path
    * @return RDD of Nodes which are objects in the files
    */
  def loadObjects(session: SparkSession, path: String): RDD[Node] = {
    loadTriplets(session, path)
      .map(triplet => triplet.getObject())
      .distinct()
  }

  /**
    * Load the triplets initially
    *
    * @param sparkSession Spark session
    * @param path Path of the NT files to read. Could be a local, HDFS, S3 path
    * @return RDD of Triples {subject, predicate, object}
    */
  def loadTriplets(sparkSession: SparkSession, path: String): RDD[Triple] = {
    val tripletsRDD = sparkSession.sparkContext.textFile(path)
      .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
      .map(line =>
        RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
    tripletsRDD
  }

  /**
    * Returns the parsed graph
    *
    * @param tripleRDD A RDD of Triples {subject, predicate, object}
    * @return Spark GraphX of Nodes
    */
  private def getSemanticGraph(tripleRDD: RDD[Triple]): org.apache.spark.graphx.Graph[Node, Node] = {
    //Generate hashCodes for graphx representation
    val extTripleRDD = tripleRDD.map(triple => (triple, triple.getSubject().hashCode().toLong, triple.getObject().hashCode().toLong))
    val subjects: RDD[(VertexId, Node)] = tripleRDD.map(triple => (triple.getSubject().hashCode().toLong, triple.getSubject()))
    val predicates: RDD[Edge[Node]] = extTripleRDD.map(line => Edge(line._2, line._3, line._1.getPredicate()))
    val objects: RDD[(VertexId, Node)] = tripleRDD.map(triple => (triple.getObject().hashCode().toLong, triple.getObject()))
    val graph = org.apache.spark.graphx.Graph(subjects.union(objects).distinct(), predicates.distinct())
    graph
  }

  /**
    * Prints the current RDD of Triples
    * Warning: this method needs to collect the RDD
    *
    * @param tripletsRDD
    */
  def showTripletsRDD(tripletsRDD: RDD[Triple]): Unit = {
    tripletsRDD.collect().foreach(println(_))
  }


  //RDF Operations
  /**
    * Returns a VertexRDD of Nodes linked by the given property
    * @param graph Spark GraphX of Nodes
    * @param property String of the property given
    * @return VertexRDD of Nodes linked by the given property
    */
  def getSubjectsWithProperty(graph: org.apache.spark.graphx.Graph[Node, Node], property: String): VertexRDD[Node] = {
    val objectPropertyId = graph.edges.filter(edge=> edge.attr.hasURI(property)).map(line => line.srcId)

    val subjectVertices = graph.vertices
      .join(objectPropertyId.keyBy((i => i)))
      .map(line => (line._1, line._2._1))
    val result = org.apache.spark.graphx.VertexRDD(subjectVertices)
    result
  }

  @deprecated
  def Dep_getSubjectsWithProperty(graph: org.apache.spark.graphx.Graph[Node, Node], property: String): VertexRDD[Node] = {
    val objectPropertyId = graph.vertices.filter(vert => vert._2.hasURI(property)).first()._1
    val vertexSubjectIds = graph.edges.filter(line => line.dstId == objectPropertyId).map(line => line.srcId)
    val subjectVertices = graph.vertices
      .join(vertexSubjectIds.keyBy((i => i)))
      .map(line => (line._1, line._2._1))
    val result = org.apache.spark.graphx.VertexRDD(subjectVertices)
    result
  }
  @deprecated
  def expandNodes(nodes: VertexRDD[Node], graph: org.apache.spark.graphx.Graph[Node, Node]): VertexRDD[Node] = {
    val vertIds = nodes.map(node => (node._1.toLong, node._2))
    val edges = graph.edges.map(line => (line.srcId, line.dstId))
    val newVertsIds = edges.join(vertIds)
      .map(line => line._2._1)
    val newVerts = graph.vertices
      .join(newVertsIds.keyBy((i => i)))
      .map(line => (line._1, line._2._1))
    org.apache.spark.graphx.VertexRDD(newVerts.union(nodes).distinct())
  }

  /**
    * Returns a dataset of rows expanded by the number of levels given
    *
    * @param nodes VertexRDD of Nodes which are a subset of the Graph Nodes
    * @param graph Spark GraphX of Nodes
    * @param levels Number of levels to expand
    * @return Dataset of Rows of expanded levels
    */
  def expandNodesNLevel(nodes: VertexRDD[Node],
                        graph: org.apache.spark.graphx.Graph[Node, Node], levels: Int = 1): Dataset[Row] = {
    import processSparkSession.implicits._

    val edges = graph.edges.map(l => (l.srcId, l.dstId)).toDF(Seq("srcId", "dstId"): _*).cache()
    var edgesR = graph.edges.map(l => (l.srcId, l.dstId, 0)).toDF(Seq("source", "level", "depth"): _*).cache()
    val nodesR = nodes.map(l => l._1).toDF(Seq("nodeId"): _*)

    var results = edgesR.distinct()

    for (level <- 1 until levels) {
      val res = edges.join(edgesR.drop("depth"), $"dstId" === $"source", "leftouter").orderBy($"srcId")
      edgesR = res.select($"srcId" as "source", $"level" as "level").withColumn("depth", lit(level))
      results = results.union(edgesR.distinct())
    }
    results = results.join(nodesR, $"source" === $"nodeId").drop($"nodeId").na.drop().distinct().orderBy($"depth", $"source")
    results
  }
}

class TripleReader(sparkSession: SparkSession, inputFile: String) extends ReaderRDF{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    //val graph = loadGraph(sparkSession, config.hdfsInputPath + "*.nt")
    val graph = loadGraph(sparkSession, inputFile)
    graph.vertices.collect().foreach(println(_))
    graph.edges.collect()foreach(println(_))

    val subjectVertices = getSubjectsWithProperty(graph, "http://dbpedia.org/ontology/deathPlace")
    val expanded = expandNodesNLevel(subjectVertices, graph, 3)
    subjectVertices.collect().foreach(println(_))
    expanded.show()
  }
}