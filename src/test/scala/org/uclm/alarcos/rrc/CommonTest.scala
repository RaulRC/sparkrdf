package org.uclm.alarcos.rrc

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

/**
  * Created by raul reguillo on 14/09/17.
  */
trait CommonTest extends FlatSpec with Matchers{

  val config: Config = ConfigFactory.load

  val getTextFromFile = (file: String) => {
    Source.fromFile(getClass.getResource(file).getPath).getLines().mkString
  }


}
