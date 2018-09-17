package com.shujia.spark.study

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka._
import redis.clients.jedis.Jedis

object CarEventCountAnalytics {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    val conf: SparkConf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))
    //ssc.checkpoint(".")


    val topics = Set("car_events")
    val brokers = "node1:9092,node2:9092,node3:9092"

    val kafkaParams = Map(
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )
    //redis卡槽位
    val dbIndex = 5

    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics)


    val linesRDD: DStream[Array[String]] = kafkaStream.map(line => line._2.split("\t"))

    val carSpeed: DStream[(String, (Int, Int))] = linesRDD
      .map(x => (x(0), x(3).toInt))
      .mapValues((x: Int) => (x, 1))
      //统计20秒内卡扣总速度和总的车流量
      .reduceByKeyAndWindow((a: (Int, Int), b: (Int, Int)) => {
        (a._1 + b._1, a._2 + b._2)
      }, Seconds(20), Seconds(10))


    carSpeed.foreachRDD(rdd => {

      rdd.foreachPartition(partitionOfRecords => {
        //获取redis链接
        val jedis: Jedis = RedisClient.pool.getResource

        partitionOfRecords.foreach(pair => {

          val camera_id: String = pair._1

          val total: Int = pair._2._1

          val count: Int = pair._2._2

          //获取当前时间
          val now: Date = Calendar.getInstance().getTime

          //时间格式化器，SimpleDateFormat不能再多线程下使用
          val minuteFormat = new SimpleDateFormat("HHmm")

          val dayFormat = new SimpleDateFormat("yyyyMMdd")
          //小时分钟
          val time: String = minuteFormat.format(now)
          //日期
          val day: String = dayFormat.format(now)
          if (count != 0) {
            //不选择卡槽位，默认往0卡槽存数据
            jedis.select(dbIndex)
            println(camera_id)
            //向redis里面存每一天每一个卡扣每小时每分钟的总的速度和总的车流量
            jedis.hset(day + "_" + camera_id, time, total + "_" + count)
          }
        })
        //将redis连接放回连接池
        RedisClient.pool.returnResource(jedis)
      })

    })
    println("spark streaming 已启动")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }


}