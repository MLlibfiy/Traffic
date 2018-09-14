package com.shujia.spark.skynet

import java.{lang, util}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.shujia.spark.constant.Constants
import com.shujia.spark.dao.IMonitorDAO
import com.shujia.spark.dao.factory.DAOFactory
import com.shujia.spark.domain.{MonitorState, Task, TopNMonitor2CarCount, TopNMonitorDetailInfo}
import com.shujia.spark.util.{ParamUtils, SparkUtilsScala, StringUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.JavaConversions._

object MonitorFlowAnalyzeScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MonitorFlowAnalyzeScala")
    SparkUtilsScala.setMaster(conf)
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = SparkUtilsScala.getSQLContext(sc)

    //会将我们的数据注册成两张临时表
    //线上运行额时候是读取hive表
    SparkUtilsScala.mockData(sc, sqlContext)

    val taskId: lang.Long = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR)

    //自定义累加器
    val monitorAndCameraStateAccumulator: Accumulator[String] = sc.accumulator[String]("")(new MonitorAndCameraStateAccumulator)


    //查询task运行时参数
    val task: Task = DAOFactory.getTaskDAO.findTaskById(taskId)

    if (task == null) return

    val params: JSONObject = JSON.parseObject(task.getTaskParams)

    //过滤执行时间范围的数据
    val cameraRDD: RDD[Row] = SparkUtilsScala.getCameraRDDByDateRange(sqlContext, params).cache()

    //转换成kv格式，k:卡口id  v：一行数据
    val monitor2DetailRDD: RDD[(String, Row)] = cameraRDD.map(row => {
      val monitorId: String = row.getString(1)
      (monitorId, row)
    })

    val monitorId2CameraCountRDD: RDD[(String, String)] = monitor2DetailRDD
      .groupByKey()
      .map(tuple => {
        val monitorId: String = tuple._1
        val iterable: Iterator[Row] = tuple._2.iterator
        val cameraList = new util.ArrayList[String]()
        var areaId = ""
        var count = 0
        while (iterable.hasNext) {
          val row: Row = iterable.next()
          val cameraId: String = row.getString(2)
          areaId = row.getString(7)
          if (!cameraList.contains(cameraId)) {
            cameraList.add(cameraId)
          }
          //统计总车流量
          count += 1
        }
        //从数据里面统计的摄像机的数量
        val normalCameraCount: Int = cameraList.size()


        val tmpInfos: String = cameraList.toList.mkString(",")

        val infos: String = Constants.FIELD_MONITOR_ID + "=" + monitorId + "|" +
          Constants.FIELD_AREA_ID + "=" + areaId + "|" +
          Constants.FIELD_CAMERA_IDS + "=" + tmpInfos.toString.substring(1) + "|" +
          Constants.FIELD_CAMERA_COUNT + "=" + normalCameraCount + "|" +
          Constants.FIELD_CAR_COUNT + "=" + count
        (monitorId, infos)
      }).cache()


    /**
      * 从monitor_camera_info表中查询出来每一个卡口对应的实际camera的数量
      */
    val sqlText = "SELECT * FROM monitor_camera_info"
    val cameraDF: DataFrame = sqlContext.sql(sqlText)

    val standardDonitor2CameraInfos: RDD[(String, String)] = cameraDF.rdd
      .map(row => (row.getString(0), row.getString(1)))
      .groupByKey()
      .map(tuple => {
        val monitorId: String = tuple._1
        val cameraList: List[String] = tuple._2.toList
        val cameraInfos: String = Constants.FIELD_CAMERA_IDS + "=" + cameraList.mkString(",") + "|" +
          Constants.FIELD_CAMERA_COUNT + "=" + cameraList.length
        (monitorId, cameraInfos)
      })


    /**
      * 1、统计正常卡口数量，正常照相机数量，异常卡口数量，异常照相机数量，异常照相机详细信息
      *
      */
    standardDonitor2CameraInfos.leftOuterJoin(monitorId2CameraCountRDD)
      .foreach(tuple => {
        val monitorId: String = tuple._1
        //真正卡口下面的摄像机详细信息
        val standardCameraInfos: String = tuple._2._1
        //从数据里面得到的卡口下面的摄像机详细信息
        val factCameraInfosOptional: Option[String] = tuple._2._2
        //实际卡口摄像机数量
        val standardCameraCount: String = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT)
        //实际卡口下面的摄像机列表
        val standardCameraIds: String = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS)

        //isDefined返回false 说明右表没关联上
        // isDefined 如果可选值是 Some 的实例返回 true，否则返回 false。
        if (factCameraInfosOptional.isDefined) {
          val factCameraInfos: String = factCameraInfosOptional.get

          //从数据里面的大的卡口下面的摄像机数量
          val factCameraCount: String = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT)
          //从从数据里面得到的卡口下面的摄像机列表
          val factCameraIds: String = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS)
          //如果相等说明这个卡口没问题
          if (standardCameraCount == factCameraCount) {

            val str: String = Constants.FIELD_NORMAL_MONITOR_COUNT + "=1|" +
              Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + standardCameraCount
            //累加
            monitorAndCameraStateAccumulator.add(str)
          } else {
            val standardCameraIdsList: List[String] = standardCameraIds.split(",").toList
            val factCameraIdsList: List[String] = factCameraIds.split(",").toList
            val abnormalMonitorCameraInfosList = new util.ArrayList[String]
            for (elem <- standardCameraIdsList) {
              //如果数据里面没有这个摄像机信息说明这个摄像机有问题
              if (!factCameraIdsList.contains(elem)) {
                //这个卡口下异常的摄像机
                abnormalMonitorCameraInfosList.add(elem)
              }
            }
            //这个卡口下异常摄像机数量
            val abnormalCameraCount: Int = standardCameraCount.toInt - factCameraCount.toInt
            //这个卡口下异常摄像机列表
            val abnormalMonitorCameraInfos: String = abnormalMonitorCameraInfosList.toList.mkString(",")

            val str: String = Constants.FIELD_NORMAL_MONITOR_COUNT + "=0|" +
              Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + factCameraCount + "|" +
              Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|" +
              Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnormalCameraCount + "|" +
              Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + abnormalMonitorCameraInfos
            //累加
            monitorAndCameraStateAccumulator.add(str)

          }
        } else {
          //没有关联上说明整个卡口都处于异常状态
          //累加器累加

          val str: String = Constants.FIELD_NORMAL_MONITOR_COUNT + "=0|" +
            Constants.FIELD_NORMAL_CAMERA_COUNT + "=0|" +
            Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|" +
            Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + standardCameraCount + "|" +
            Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + standardCameraIds
          //累加
          monitorAndCameraStateAccumulator.add(str)
        }


      })

    //保存数据到数据库
    saveMonitorState(taskId, monitorAndCameraStateAccumulator)


    /**
      * 2、获取卡口流量的前N名，并且持久化到数据库中
      *
      */

    //从数据库里面查询top值
    val topNumFromParams: Int = ParamUtils.getParam(params, Constants.FIELD_TOP_NUM).toInt

    val topNFlow: Array[(String, String)] = monitorId2CameraCountRDD.map(tuple => {
      val monitorId: String = tuple._1
      val factCameraInfosOptional: String = tuple._2
      //获取卡口的车流量
      val carCount: String = StringUtils.getFieldFromConcatString(factCameraInfosOptional, "\\|", Constants.FIELD_CAR_COUNT)
      (carCount, monitorId)
    }).sortByKey(ascending = false) //车流量倒序排序
      .take(topNumFromParams)

    val list = new util.ArrayList[TopNMonitor2CarCount]()

    topNFlow.foreach(tuple => {
      val carCount: String = tuple._1
      val monitorId: String = tuple._2
      val monitor2CarCount = new TopNMonitor2CarCount(taskId, monitorId, carCount.toInt)
      list.add(monitor2CarCount)
    })

    //插入数据到数据库
    DAOFactory.getMonitorDAO.insertBatchTopN(list)


    /**
      * 3、查询topN 卡口的详细信息
      *
      *
      * private var taskId: Long = 0L
      * private var date: String = null
      * private var monitorId: String = null
      * private var cameraId: String = null
      * private var car: String = null
      * private var actionTime: String = null
      * private var speed: String = null
      * private var roadId: String = null
      *
      */

    val topNMonitorIds: Array[String] = topNFlow.map(_._2)

    val broadcast: Broadcast[Array[String]] = sc.broadcast(topNMonitorIds)
    val topNMonitorInfo: RDD[(String, Row)] = monitor2DetailRDD.filter(tuple => broadcast.value.contains(tuple._1))

    val infoes = new util.ArrayList[TopNMonitorDetailInfo]()

    topNMonitorInfo.map(tuple => tuple._2)
      .collect()
      .foreach(row => {
        val m = new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6))
        infoes.add(m)
      })
    //插入到数据库
    val monitorDAO: IMonitorDAO = DAOFactory.getMonitorDAO
    monitorDAO.insertBatchMonitorDetails(infoes)


  }


  private def saveMonitorState(taskId: Long, monitorAndCameraStateAccumulator: Accumulator[String]): Unit = {
    /**
      * 累加器中值能在Executor段读取吗？
      * 不能
      * 这里的读取时在Driver中进行的
      */
    val accumulatorVal: String = monitorAndCameraStateAccumulator.value
    val normalMonitorCount: String = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_MONITOR_COUNT)
    val normalCameraCount: String = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_CAMERA_COUNT)
    val abnormalMonitorCount: String = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_COUNT)
    val abnormalCameraCount: String = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_CAMERA_COUNT)
    val abnormalMonitorCameraInfos: String = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS)
    /**
      * 这里面只有一条记录
      */
    val monitorState = new MonitorState(taskId, normalMonitorCount, normalCameraCount, abnormalMonitorCount, abnormalCameraCount, abnormalMonitorCameraInfos)
    val monitorDAO: IMonitorDAO = DAOFactory.getMonitorDAO
    //插入到数据库
    monitorDAO.insertMonitorState(monitorState)
  }
}