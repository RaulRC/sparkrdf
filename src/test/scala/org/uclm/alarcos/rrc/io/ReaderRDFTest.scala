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
    val depth = 3

    val result = step.expandNodesNLevel(graph.vertices, graph, depth)

    graph.vertices.collect().foreach(println(_))
    result.collect().foreach(println(_))

    assert(true)

  }
}
