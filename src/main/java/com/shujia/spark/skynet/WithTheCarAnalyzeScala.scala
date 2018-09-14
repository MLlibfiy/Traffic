package com.shujia.spark.skynet

import java.{lang, util}

import com.shujia.spark.constant.Constants
import com.shujia.spark.dao.factory.DAOFactory
import com.shujia.spark.domain.{CarTrack, Task}
import com.shujia.spark.util.{DateUtils, ParamUtils, SparkUtils, SparkUtilsScala}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Row, SQLContext}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.shujia.spark.dao.{ICarTrackDAO, ITaskDAO}
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
    val conf: SparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)


    SparkUtilsScala.setMaster(conf)
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = SparkUtilsScala.getSQLContext(sc)

    /**
      * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的临时表就可以
      * 本地模拟数据注册成一张临时表
      * monitor_flow_action
      */
    SparkUtilsScala.mockData(sc, sqlContext)
    //从配置文件中查询出来指定的任务ID
    val taskId: lang.Long = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_WITH_THE_CAR)
    /**
      * 通过taskId从数据库中查询相应的参数
      * 1、通过DAOFactory工厂类创建出TaskDAO组件
      * 2、查询task
      */
    val taskDAO: ITaskDAO = DAOFactory.getTaskDAO
    val task: Task = taskDAO.findTaskById(taskId)
    if (task == null) return
    /**
      * task对象已经获取到，因为params是一个json，所以需要创建一个解析json的对象
      */
    val taskParamsJsonObject: JSONObject = JSON.parseObject(task.getTaskParams)
    /**
      * 统计出指定时间内的车辆信息
      */
    val cameraRDD: RDD[Row] = SparkUtilsScala.getCameraRDDByDateRangeAndCars(sqlContext, taskParamsJsonObject)

    withTheCarAnalyze(taskId, sc, cameraRDD)
    sc.stop()
  }

  private def withTheCarAnalyze(taskId: Long, sc: SparkContext, cameraRDD: RDD[Row]): Unit = {
    /**
      * trackWithActionTimeRDD
      * k: car
      * v:monitor_id+"_"+action_time
      */
    val trackWithActionTimeRDD: RDD[(String, String)] = getCarTrack(cameraRDD)

    /**
      * 所有车辆轨迹存储在MySQL中，测试只是放入到MySQL   实际情况是在Redis中
      * car	monitor:actionTime
      *
      * monitor_id	时间段（actionTime）
      */
    trackWithActionTimeRDD.foreachPartition(iterator => {
      val carTracks = new util.ArrayList[CarTrack]
      while (iterator.hasNext) {
        val tuple: (String, String) = iterator.next
        val car: String = tuple._1
        val timeAndTack: String = tuple._2

        carTracks.add(new CarTrack(taskId, DateUtils.getTodayDate, car, timeAndTack))
      }
      val carTrackDAO: ICarTrackDAO = DAOFactory.getCarTrackDAO
      carTrackDAO.insertBatchCarTrack(carTracks)
    })

  }

  def getCarTrack(cameraRDD: RDD[Row]): RDD[(String, String)] = {

    val resuRDD: RDD[(String, String)] = cameraRDD.map(row => (row.getString(3), row))
      .groupByKey()
      .map(tuple => {
        val iterator: Iterator[Row] = tuple._2.iterator
        val car: String = tuple._1

        val trackWithTime: String = iterator.toList.sortWith((r1, r2) => {
          val actionTime1: String = r1.getString(4)
          val actionTime2: String = r2.getString(4)
          DateUtils.before(actionTime1, actionTime2)
        }).map(row => row.getString(1) + "=" + row.getString(4)).mkString("|")

        (car, trackWithTime)
      })
    resuRDD
  }
}
