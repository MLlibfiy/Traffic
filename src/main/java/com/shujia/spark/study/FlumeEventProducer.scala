package com.shujia.spark.study

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import scala.io.Source


object FlumeEventProducer {

  def main(args: Array[String]): Unit = {

    val TrafficDataHome = "/root/Traffic/data/"

    val filePath: String = TrafficDataHome + "in/2018082013_all_column_test.log"
    val outPath: String = TrafficDataHome + "out/2018082013_all_column_test.log"

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outPath), "utf-8"))

    println("开始生成日志")
    for (line <- Source.fromFile(filePath).getLines()) {
      writer.write(line)
      writer.newLine()
      writer.flush()
      Thread.sleep(200)
    }
    writer.close()

  }
}