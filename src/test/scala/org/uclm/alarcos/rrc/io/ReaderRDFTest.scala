package org.uclm.alarcos.rrc.io


import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.uclm.alarcos.rrc.CommonTest
import org.uclm.alarcos.rrc.config.DQAssessmentConfiguration
import org.uclm.alarcos.rrc.spark.SparkSpec

/**
  * Created by raul.reguillo on 12/06/17.
  */
@RunWith(classOf[JUnitRunner])
class ReaderRDFTest extends CommonTest with SparkSpec with MockFactory {

  "Execute expandNodesNLevel" should "be succesfully" in {

    val testPath = "src/test/resources/dataset/*"

    object MockedTripleReader extends TripleReader(spark, testPath)

    val step = MockedTripleReader

    val depth = 3

    //val result = step.expandNodesNLevel(null, null, depth)

    assert(true)

  }
}
