package com.spark.spark.test

import com.shujia.spark.skynet.MonitorAndCameraStateAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

/**
  * Created by 98726 on 2017/3/8.
  */
object test {
  def main(args: Array[String]) {
   /*/* var sc = new SparkContext(new SparkConf())

    sc.accumulable(new MonitorAndCameraStateAccumulator())*/
   var  m = new HashMap[String,Long]
    m.+=("q" -> 123)
    m.+=("2" -> 123)
    m.+=("3" -> 123)
    m.+=("4" -> 123)
    m.+=("5" -> 123)
    m.+=("5" -> 123)

    m.foreach(println)*/


  }
}
