package org.uclm.alarcos.rrc.config

import com.typesafe.config.Config

/**
  * Created by Raul Reguillo on 30/08/17.
  */
class DQAssessmentConfiguration(env: String, config: Config) extends Serializable{

  val masterMode = config.getString(s"$env.masterMode")
  val hdfsOutputPath = config.getString(s"$env.hdfs.outputPath")
  val hdfsInputPath =  config.getString(s"$env.hdfs.inputPath")


}

object DQAssessmentConfiguration {

  /**
    * Returns the configuration for a specific environment
    * @param env Name of the environment
    * @param config config
    * @return the configuration for Arrowhead steps
    */
  def apply(env:String, config: Config) =
    new DQAssessmentConfiguration(env, config)
}