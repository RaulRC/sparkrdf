package org.uclm.alarcos.rrc.io


import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.jena.graph.Node
import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.uclm.alarcos.rrc.CommonTest
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.spark.SparkSpec
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
/**
  * Created by raul.reguillo on 12/06/17.
  */
@RunWith(classOf[JUnitRunner])
class ReaderRDFTest extends CommonTest with SparkSpec with MockFactory {

  "Execute expandNodesNLevel" should "be succesfully" in {

    val testPath = "src/test/resources/dataset/tinysample.nt"



    object MockedTripleReader extends TripleReader(spark, testPath)

    val step = MockedTripleReader
    val graph = step.loadGraph(spark, testPath)
    val depth = 4

    val result = step.expandNodesNLevel(graph.vertices, graph, depth)

    graph.vertices.collect().foreach(println(_))
    graph.edges.collect().foreach(println(_))
    println("--")
    result.collect().foreach(println(_))

    val results = result.collect()
//    A -> B -> D -> F -> G
//         | \  |
//         v  \,v
//         C -> E

    //Nodes IDS
    val A = 293150257L
    val B = 293150288L
    val C = 293150319L
    val D = 293150350L
    val E = 293150381L
    val F = 293150412L
    val G = 293150443L


    //LVL 0
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === A & l.get(1).asInstanceOf[Long] === B & l.get(2) === 0).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === B & l.get(1).asInstanceOf[Long] === C & l.get(2) === 0).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === B & l.get(1).asInstanceOf[Long] === D & l.get(2) === 0).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === B & l.get(1).asInstanceOf[Long] === E & l.get(2) === 0).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === C & l.get(1).asInstanceOf[Long] === E & l.get(2) === 0).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === D & l.get(1).asInstanceOf[Long] === F & l.get(2) === 0).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === F & l.get(1).asInstanceOf[Long] === G & l.get(2) === 0).length === 1)
    //LVL 1
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === A & l.get(1).asInstanceOf[Long] === C & l.get(2) === 1).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === A & l.get(1).asInstanceOf[Long] === D & l.get(2) === 1).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === A & l.get(1).asInstanceOf[Long] === E & l.get(2) === 1).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === B & l.get(1).asInstanceOf[Long] === F & l.get(2) === 1).length === 1)
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === D & l.get(1).asInstanceOf[Long] === G & l.get(2) === 1).length === 1)
    //LVL 2
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === A & l.get(1).asInstanceOf[Long] === F & l.get(2) === 2).length === 1)
    //LVL 3
    assert(results.filter(l => l.get(0).asInstanceOf[Long] === A & l.get(1).asInstanceOf[Long] === G & l.get(2) === 3).length === 1)


  }
}
