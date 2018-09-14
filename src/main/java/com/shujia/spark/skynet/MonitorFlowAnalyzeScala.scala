package com.shujia.spark.skynet

import java.util

import com.alibaba.fastjson.JSON
import com.shujia.spark.constant.Constants
import com.shujia.spark.dao.factory.DAOFactory
import com.shujia.spark.util.{ParamUtils, SparkUtilsScala, StringUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import scala.collection.JavaConversions._
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

    //自定义累加器
    val monitorAndCameraStateAccumulator = sc.accumulator[String]("")(new MonitorAndCameraStateAccumulator)


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

    val monitorId2CameraCountRDD = monitor2DetailRDD
      .groupByKey()
      .map(tuple => {
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


        val tmpInfos = cameraList.toList.mkString(",")

        val infos = Constants.FIELD_MONITOR_ID + "=" + monitorId + "|" +
          Constants.FIELD_AREA_ID + "=" + areaId + "|" +
          Constants.FIELD_CAMERA_IDS + "=" + tmpInfos.toString.substring(1) + "|" +
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
        (monitorId, cameraInfos)
      })


    standardDonitor2CameraInfos.leftOuterJoin(monitorId2CameraCountRDD)
      .foreach(tuple => {
        val monitorId = tuple._1
        //真正卡口下面的摄像机详细信息
        val standardCameraInfos = tuple._2._1
        //从数据里面得到的卡口下面的摄像机详细信息
        val factCameraInfosOptional = tuple._2._2
        //实际卡口摄像机数量
        val standardCameraCount = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT)
        //实际卡口下面的摄像机列表
        val standardCameraIds = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS)
        var factCameraInfos = ""

        if (factCameraInfosOptional.isDefined) {
          factCameraInfos = factCameraInfosOptional.get

          //从数据里面的大的卡口下面的摄像机数量
          val factCameraCount = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT)
          //从从数据里面得到的卡口下面的摄像机列表
          val factCameraIds = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS)
          //如果相等说明这个卡口没问题
          if (standardCameraCount == factCameraCount) {
            println(standardCameraCount)
                      val str = Constants.FIELD_NORMAL_MONITOR_COUNT + "=1|" +
                        Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + standardCameraCount + "|"
                      monitorAndCameraStateAccumulator.add(str)
          } else {
            val standardCameraIdsList =  standardCameraIds.split("").toList
            val factCameraIdsList =  factCameraIds.split("").toList

            val abnormalMonitorCameraInfosList = new util.ArrayList[String]

            for (elem <- standardCameraIdsList) {
              //如果数据里面没有这个摄像机信息说明这个摄像机有问题
              if (!factCameraIdsList.contains(elem)){
                //这个卡口下异常的摄像机
                abnormalMonitorCameraInfosList.add(elem)

              }

            }
            //这个卡口下异常摄像机数量
            val abnormalCameraCount = standardCameraCount.toInt - factCameraCount.toInt
            //这个卡口下异常摄像机列表
            val abnormalMonitorCameraInfos = abnormalMonitorCameraInfosList.toList.mkString(",")

            val str = Constants.FIELD_NORMAL_MONITOR_COUNT + "=0|" +
              Constants.FIELD_NORMAL_CAMERA_COUNT + "="+factCameraCount+"|" +
              Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|" +
              Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnormalCameraCount + "|" +
              Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + abnormalMonitorCameraInfos
            monitorAndCameraStateAccumulator.add(str)

          }

        } else {
          //没有关联上说明整个卡口都处于异常状态
          //累加器累加

          val str = Constants.FIELD_NORMAL_MONITOR_COUNT + "=0|" +
            Constants.FIELD_NORMAL_CAMERA_COUNT + "=0|" +
            Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|" +
            Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + standardCameraCount + "|" +
            Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + standardCameraIds
          monitorAndCameraStateAccumulator.add(str)
        }


      })

    val accumulator = monitorAndCameraStateAccumulator.localValue
    print(accumulator)
  }
}