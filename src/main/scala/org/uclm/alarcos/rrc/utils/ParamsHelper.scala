package org.uclm.alarcos.rrc.utils

/**
  * Created by Raul Reguillo on 31/08/17.
  */
object ParamsHelper {
  def getParams(args: Array[String]): Params = {
    Params(args(0), args(1))
  }
}

case class Params(env: String, inputFile: String)

