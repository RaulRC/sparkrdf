package org.uclm.alarcos.rrc.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by raul reguillo on 14/09/17.
  */
trait SparkSpec extends BeforeAndAfterAll {


  this: Suite =>

  private var _spark: SparkSession = _
  private val master = "local"
  private val appName = "test"


  override def beforeAll(): Unit = {

    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    super.beforeAll()

    _spark = SparkSession.builder().config(conf).getOrCreate()
  }


  override def afterAll(): Unit = {

    if ( _spark != null) {
      _spark.stop()
      _spark = null
    }
    super.afterAll()
  }

  def spark: SparkSession =  _spark


}