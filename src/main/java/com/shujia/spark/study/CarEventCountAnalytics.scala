package com.shujia.spark.study

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.google.gson.Gson
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka._
import org.json.JSONObject
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

    val dbIndex = 3

    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val linesRDD: DStream[Array[String]] = kafkaStream.map(line => line._2.split("\t"))

    val carSpeed: DStream[(String, (Int, Int))] = linesRDD.map(x => (x(0), x(3).toInt))
      .mapValues((x: Int) => (x, 1))
      .reduceByKeyAndWindow((a: (Int, Int), b: (Int, Int)) => {
        (a._1 + b._1, a._2 + b._2)
      }, Seconds(20), Seconds(10))

    carSpeed.foreachRDD(rdd => {

      rdd.foreachPartition(partitionOfRecords => {
        val jedis: Jedis = RedisClient.pool.getResource
        partitionOfRecords.foreach(pair => {
          val camera_id: String = pair._1
          val total: Int = pair._2._1
          val count: Int = pair._2._2
          val now: Date = Calendar.getInstance().getTime
          val minuteFormat = new SimpleDateFormat("HHmm")
          val dayFormat = new SimpleDateFormat("yyyyMMdd")
          val time: String = minuteFormat.format(now)
          val day: String = dayFormat.format(now)
          if (count != 0) {
            jedis.select(dbIndex)
            println(camera_id)
            jedis.hset(day + "_" + camera_id, time, total + "_" + count)
          }
        })
        RedisClient.pool.returnResource(jedis)
      })

    })
    println("=" * 20)
    ssc.start()
    ssc.awaitTermination()

  }


}