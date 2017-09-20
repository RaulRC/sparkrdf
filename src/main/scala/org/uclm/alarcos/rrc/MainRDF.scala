package org.uclm.alarcos.rrc

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.uclm.alarcos.rrc.configrdf.SparkRDFConfiguration
import org.uclm.alarcos.rrc.io.{ReaderRDF, WriterRDF}
import org.uclm.alarcos.rrc.utilsrdf.ParamsHelper

/**
  * Created by Raul Reguillo on 31/08/17.
  */
object MainRDF {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(getClass)
    logger.info("Main starting...")

    implicit val config: Config = ConfigFactory.load

    val environments = config.getStringList("environments")

    /**
      * input: <environment> <file>
      */
    if (args.length == 0) {
      logger.error(s"Environment is mandatory. Valid environments are: $environments")
      System.exit(1)
    }
    implicit val params = ParamsHelper.getParams(args)
    implicit val env = params.env
    implicit val inputFile = params.inputFile

    if (!environments.contains(env)) {
      logger.error(s"Environment $env not allowed. Valid environments are: $environments")
      System.exit(0)
    }

    logger.info("Create Context for " + env)
    logger.info("Configuration file loaded..." + config.getConfig(env))

    val loadedConfig = SparkRDFConfiguration.apply(env, config)

    val sparkConf = new SparkConf()
      .setAppName("SparkRDF")
      .setMaster(loadedConfig.masterMode)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()


    logger.info("Loading class " + "SparkRDF")
    launchStep(Class.forName(s"org.uclm.alarcos.rrc.io.TripleReader")) (spark, inputFile)

  }

  /**
    * Launch a specific class
    *
    * @param clazz: Class
    * @param args: Arguments received
    * @tparam T
    * @return Launching classs
    */
  def launchStep[T](clazz: java.lang.Class[T])(args: AnyRef*): T = {
    val constructor = clazz.getConstructors()(0)
    val instance = constructor.newInstance(args: _*).asInstanceOf[T]
    instance.asInstanceOf[ReaderRDF].execute()
    instance
  }


}
