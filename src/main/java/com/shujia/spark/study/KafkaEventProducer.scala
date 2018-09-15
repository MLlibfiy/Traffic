package com.shujia.spark.study

import java.sql.Timestamp
import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jettison.json.JSONObject

import scala.io.Source

object KafkaEventProducer {

  // bin/kafka-topics.sh --zookeeper node1:2181,node2:2181,node3:2181 --create --topic car_events --replication-factor 2 --partitions 2
  // bin/kafka-topics.sh --zookeeper node1:2181,node2:2181,node3:2181 --list
  // bin/kafka-topics.sh --zookeeper node1:2181,node2:2181,node3:2181 --describe car_events
  def main(args: Array[String]): Unit = {
    val topic = "car_events"
    val brokers = "node1:9092,node2:9092,node3:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    //    val filePath = "D:/traffic/trafficlf_all_column_all.txt"
    val filePath = "E:\\bigdata\\spark\\Traffic\\data\\2018082013_all_column_test.txt"

    val records = Source.fromFile(filePath).getLines()
      .filter(!_.startsWith(";"))
      .map(_.replace("'", ""))
      .map(_.split(","))
      .map(record => (record(0).trim, record(2).trim, record(4).trim, record(6).trim, record(13).trim))

     for (record <- records) {
       // prepare event data
       val event = new JSONObject()
       event
         .put("camera_id", record._1)
         .put("car_id", record._1)
         .put("event_time", record._3)
         .put("speed", record._4)
         .put("road_id", record._5)

       // produce event message
       producer.send(new KeyedMessage[String, String](topic, event.toString))
       println("Message sent: " + event)

       Thread.sleep(200)
     }
  }
}