package com.shujia.spark.study

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer


object PredictLRwithLBFGS {

    val sparkConf = new SparkConf().setAppName("Shanghai traffic").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val dayFormat = new SimpleDateFormat("yyyyMMdd")
    val minuteFormat = new SimpleDateFormat("HHmm")
    val sdf = new SimpleDateFormat( "yyyy-MM-dd_HH:mm:ss" );

    def main(args: Array[String]) {

        val input = "2018-09-16_13:40:00"
        val date: Date = sdf.parse( input )
        val inputTimeLong: Long = date.getTime
        val inputTime = new Date(inputTimeLong)
        val day: String = dayFormat.format(inputTime)

        // fetch data from redis
        val jedis: Jedis = RedisClient.pool.getResource
        jedis.select(3)

        val camera_ids = List("310999003001","310999003102")
        val camera_relations:Map[String,Array[String]] = Map[String,Array[String]](
            "310999003001" -> Array("310999003001","310999003102","310999000106","310999000205","310999007204"),
            "310999003102" -> Array("310999003001","310999003102","310999000106","310999000205","310999007204")
        )

        val temp: List[lang.Long] = camera_ids.map({ camera_id =>
            val list = camera_relations(camera_id)

            val relations: Array[(String, util.Map[String, String])] = list.map({ camera_id =>
                println(day + "_" + camera_id)
                (camera_id, jedis.hgetAll(day + "_" + camera_id))

            })

            relations.foreach(println)


            val featuresX: ArrayBuffer[Double] = ArrayBuffer[Double]()
            // 获取当前分钟最近两分钟的数据
            for(index <- 0 to 2){
                val tempOne: Long = inputTimeLong - 60 * 1000 * index
                val tempMinute: String = minuteFormat.format(tempOne)
                for((k,v) <- relations){
                    val map: util.Map[String, String] = v
                    if (map.containsKey(tempMinute)){
                        val info: Array[String] = map.get(tempMinute).split("_")
                        val f: Float = info(0).toFloat / info(1).toFloat
                        featuresX += f
                    } else{
                        featuresX += -1.0
                    }
                }
            }


            // 运行训练算法构建模型
            val path: String = jedis.hget("model",camera_id)
            val model: LogisticRegressionModel = LogisticRegressionModel.load(sc, path)

            val prediction: Double = model.predict(Vectors.dense(featuresX.toArray))
            println(input+"\t"+camera_id+"\t"+prediction+"\t")
            jedis.hset(input, camera_id, prediction.toString)
        })

        RedisClient.pool.returnResource(jedis)
    }
 }
