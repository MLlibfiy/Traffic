package com.shujia.spark.skynet

import java.util

import com.alibaba.fastjson.JSON
import com.shujia.spark.constant.Constants
import com.shujia.spark.dao.factory.DAOFactory
import com.shujia.spark.util.{ParamUtils, SparkUtilsScala}
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

object MonitorFlowAnalyzeScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MonitorFlowAnalyzeScala")

    SparkUtilsScala.setMaster(conf)

    val sc = new SparkContext(conf)

    val sqlContext = SparkUtilsScala.getSQLContext(sc)

    //会将我们的数据注册成两张临时表
    //线上运行额时候是读取hive表
    SparkUtilsScala.mockData(sc, sqlContext)

    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR)

    //查询task运行时参数
    val task = DAOFactory.getTaskDAO.findTaskById(taskId)

    if (task == null) return

    val params = JSON.parseObject(task.getTaskParams)

    //过滤执行时间范围的数据
    val cameraRDD = SparkUtilsScala.getCameraRDDByDateRange(sqlContext, params).cache()


    val monitor2DetailRDD = cameraRDD.map(row => {
      val monitorId = row.getString(1)
      (monitorId, row)
    })

    val monitorId2CameraCountRDD = monitor2DetailRDD.groupByKey().map(tuple => {
      val monitorId = tuple._1
      val iterable = tuple._2.iterator
      val cameraList = new util.ArrayList[String]()
      var areaId = ""
      var count = 0
      while (iterable.hasNext) {
        val row = iterable.next()
        val cameraId = row.getString(2)
        areaId = row.getString(7)
        if (!cameraList.contains(cameraId)) {
          cameraList.add(cameraId)
        }
        //统计总车流量
        count += 1
      }
      //从数据里面统计的摄像机的数量
      val normalCameraCount = cameraList.size()
      import scala.collection.JavaConversions._

      val tmpInfos = cameraList.toList.mkString(",")

      val infos = Constants.FIELD_MONITOR_ID + "=" + monitorId + "|" +
        Constants.FIELD_AREA_ID + "=" + areaId + "|" +
        Constants.FIELD_CAMERA_IDS + "=" +
        tmpInfos.toString.substring(1) + "|" +
        Constants.FIELD_CAMERA_COUNT + "=" + normalCameraCount + "|" +
        Constants.FIELD_CAR_COUNT + "=" + count
      (monitorId, infos)
    })


    /**
      * 从monitor_camera_info表中查询出来每一个卡口对应的实际camera的数量
      */
    val sqlText = "SELECT * FROM monitor_camera_info"
    val cameraDF = sqlContext.sql(sqlText)

    val standardDonitor2CameraInfos = cameraDF.rdd
      .map(row => (row.getString(0), row.getString(1)))
      .groupByKey()
      .map(tuple => {
        val monitorId = tuple._1
        val cameraList = tuple._2.toList
        val cameraInfos = Constants.FIELD_CAMERA_IDS + "=" + cameraList.mkString(",") + "|" +
          Constants.FIELD_CAMERA_COUNT + "=" + cameraList.length
        (monitorId,cameraInfos)
      })


  }

}