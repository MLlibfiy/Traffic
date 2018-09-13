package com.shujia.spark.rtmroad

import java.text.SimpleDateFormat
import java.util.{Calendar, List}

import com.shujia.spark.conf.ConfigurationManager
import com.shujia.spark.constant.Constants
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.io.Source

object RoadRealTimeAnalyzeScala {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("AdClickRealTimeStatSpark")

    val ssc = new StreamingContext(conf, Durations.seconds(5))

    ssc.checkpoint("E:\\第一期\\大数据\\spark\\项目\\Traffic\\out\\streaming_checkpoint")

    val brokers: String = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST)
    val kafkaParams = Map(
      "metadata.broker.list" -> brokers
    )

    // 构建topic set
    val kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)

    val topics = kafkaTopics.split(",").toSet

    val carRealTimeLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics)

    /**
      * 实时计算道路的拥堵情况
      */
//    realTimeCalculateRoadState(carRealTimeLogDStream)
    /**
      * 缉查布控，在各个卡扣中布控某一批嫌疑车辆的车牌
      */

    val path = "E:\\第一期\\大数据\\spark\\项目\\Traffic\\in\\ControlCar.txt"
    Source.fromFile(path).getLines().foreach(println)


    val resultDStream = controlCars(path, carRealTimeLogDStream)

    resultDStream.print()


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

  /**
    * 缉查布控
    *
    * @param path
    * @param carRealTimeLogDStream
    */
  def controlCars(path: String, carRealTimeLogDStream: InputDStream[(String, String)]) : DStream[String] = {

    carRealTimeLogDStream.transform(rdd=>{
      //动态修改广播变量的值
      val carList = Source.fromFile(path).getLines().toList
      val broadcast = rdd.context.broadcast(carList)

      //实际生产过程是连接数据库获取车牌号列表

      rdd.filter(x=>{

        val log: String = x._2
        val car: String = log.split("\t")(3)
        val value = broadcast.value
        value.contains(car)
      }).map(_._2)
    })
  }

  def realTimeCalculateRoadState(carRealTimeLogDStream: InputDStream[(String, String)]): Unit = {


    val count = (v1: (Int, Int), v2: (Int, Int)) => {
      (v1._1 + v2._1, v1._2 + v2._2)
    }

    val sub = (v1: (Int, Int), v2: (Int, Int)) => {
      (v1._1 - v2._1, v1._2 - v2._2)
    }

    carRealTimeLogDStream
      .map(_._2.split("\t"))
      .map(x => {
        val monitorId = x(1)
        val speed = x(5).toInt
        (monitorId, speed)
      }).mapValues((_, 1))

      /**
        * 使用reduceByKeyAndWindow  窗口大小是1分钟，为什么不讲batch Interval设置为一分钟呢？
        * 1、batch Interval  1m   每隔1m才会计算一次。 这无法改变，如果使用reduceByKeyAndWindow 但是呢batch interval 5s，每隔多少秒计算，可以自己来指定。
        * 2、如果你的application还有其他的功能点的话，另外一个功能点不能忍受这个长的延迟。权衡一下还是使用reduceByKeyAndWindow。
        */
      .reduceByKeyAndWindow(count, sub, Durations.minutes(1), Durations.seconds(10))

      .foreachRDD(rdd => {
        rdd.foreachPartition(x => {
          val secondFormate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
          while (x.hasNext) {
            val tuple = x.next
            val monitor = tuple._1
            val speedCount = tuple._2._1
            val carCount = tuple._2._2
            val meanSpeed = speedCount / carCount
            println("时间：" + secondFormate.format(Calendar.getInstance.getTime) +
              ",卡扣编号：" + monitor +
              ",车辆总数：" + carCount +
              ",速度总数：" + speedCount +
              ",平均数度：" + meanSpeed
            )
          }

          //实际上需要写入mysql
        })


      })


  }
}
