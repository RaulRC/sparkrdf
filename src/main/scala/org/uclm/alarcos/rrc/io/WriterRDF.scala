package org.uclm.alarcos.rrc.io

import java.io.ByteArrayInputStream

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql._
import org.apache.jena.graph._
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.uclm.alarcos.rrc.configrdf.SparkRDFConfiguration

/**
  * Created by raulreguillo on 6/09/17.
  */
trait WriterRDF extends Serializable{
  protected val log: Logger = LogManager.getLogger(this.getClass.getName)
  protected val processSparkSession: SparkSession

  def execute()

  def writeRDDTriplet(tripletRDD: RDD[Triple]): Unit = {
    //TODO
  }
  def showTripletsRDD(tripletsRDD: RDD[Triple]): Unit = {
    tripletsRDD.collect().foreach(println(_))
  }
}

class TripleWriter(config: SparkRDFConfiguration, sparkSession: SparkSession, period: String) extends WriterRDF{
  protected val processSparkSession: SparkSession = sparkSession

  def execute(): Unit = {
    //TODO
  }
}