package com.shujia.spark.skynet

import java.util
import java.util.{ArrayList, Collections, Comparator, Iterator, List}

import com.shujia.spark.constant.Constants
import com.shujia.spark.dao.factory.DAOFactory
import com.shujia.spark.domain.CarTrack
import com.shujia.spark.util.{DateUtils, ParamUtils, SparkUtils, SparkUtilsScala}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Row
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD


object WithTheCarAnalyzeScala {
  def main(args: Array[String]): Unit = {
    /**
      * 现在要计算的是所有的车的跟踪信息
      *
      * 标准是：两个车的时间差在5分钟内就是有跟踪嫌疑
      * table1：car track   1：时间段（精确到5分钟） 2 3 4 5
      * table2：monitor_id 12:00-12:05  cars  A B C
      * 12:06-12:10  cars
      */
    // 构建Spark上下文
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).set("spark.sql.shuffle.partitions", "10") //						.set("spark.default.parallelism", "100")
    //						.set("spark.storage.memoryFraction", "0.5")
    //						.set("spark.shuffle.consolidateFiles", "true")
    //						.set("spark.shuffle.file.buffer", "64")
    //						.set("spark.shuffle.memoryFraction", "0.3")
    //						.set("spark.reducer.maxSizeInFlight", "24")
    //						.set("spark.shuffle.io.maxRetries", "60")
    //						.set("spark.shuffle.io.retryWait", "60")
    //						.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    SparkUtils.setMaster(conf)
    val sc = new SparkContext(conf)
    val sqlContext = SparkUtilsScala.getSQLContext(sc)

    /**
      * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的临时表就可以
      * 本地模拟数据注册成一张临时表
      * monitor_flow_action
      */
    SparkUtilsScala.mockData(sc, sqlContext)
    //从配置文件中查询出来指定的任务ID
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_WITH_THE_CAR)
    /**
      * 通过taskId从数据库中查询相应的参数
      * 1、通过DAOFactory工厂类创建出TaskDAO组件
      * 2、查询task
      */
    val taskDAO = DAOFactory.getTaskDAO
    val task = taskDAO.findTaskById(taskId)
    if (task == null) return
    /**
      * task对象已经获取到，因为params是一个json，所以需要创建一个解析json的对象
      */
    val taskParamsJsonObject = JSON.parseObject(task.getTaskParams)
    /**
      * 统计出指定时间内的车辆信息
      */
    val cameraRDD = SparkUtilsScala.getCameraRDDByDateRange(sqlContext, taskParamsJsonObject)
    withTheCarAnalyze(taskId, sc, cameraRDD)
    sc.stop()
  }

  private def withTheCarAnalyze(taskId: Long, sc: JavaSparkContext, cameraRDD: RDD[Row]): Unit = {
    /**
      * trackWithActionTimeRDD
      * k: car
      * v:monitor_id+"_"+action_time
      */
    val trackWithActionTimeRDD = getCarTrack(cameraRDD)

    /**
      * 所有车辆轨迹存储在MySQL中，测试只是放入到MySQL   实际情况是在Redis中
      *
      *
      *
      * car	monitor:actionTime
      *
      * monitor_id	时间段（actionTime）
      */
//    trackWithActionTimeRDD.foreachPartition(iterator => {
//      val carTracks = new util.ArrayList[CarTrack]
//      while (iterator.hasNext) {
//        val tuple = iterator.next
//        val car = tuple._1
//        val timeAndTack = tuple._2
//
//        carTracks.add(new CarTrack(taskId, DateUtils.getTodayDate, car, timeAndTack))
//      }
//      val carTrackDAO = DAOFactory.getCarTrackDAO
//      carTrackDAO.insertBatchCarTrack(carTracks)
//    })


    /**
      * 卡口号	时间段（粒度至5分钟）	车牌集合
      * 具体查看每一个卡口在每一个时间段内车辆的数量。
      * 实现思路：
      * 按照卡口进行聚合
      */

    cameraRDD.map(row => (row.getString(1), row))
      .groupByKey()
      .foreach(tuple => {
        val monitor = tuple._1
        val rowIterator = tuple._2.iterator
        val rows = new util.ArrayList[Row]
        while ( {
          rowIterator.hasNext
        }) {
          val row = rowIterator.next
          rows.add(row)
        }
      })
  }

  def getCarTrack(cameraRDD: RDD[Row]):RDD[(String,String)] = {

    val resuRDD = cameraRDD.map(row => (row.getString(3), row))
      .groupByKey()
      .map(tuple => {
        val iterator = tuple._2.iterator
        val car = tuple._1

        val trackWithTime = iterator.toList.sortWith((r1,r2)=>{
          val actionTime1 = r1.getString(4)
          val actionTime2 = r2.getString(4)
          DateUtils.before(actionTime1, actionTime2)
        }).map(row=>row.getString(1)+"="+row.getString(4)).mkString("|")

        (car, trackWithTime)
      })
    resuRDD
  }
}
