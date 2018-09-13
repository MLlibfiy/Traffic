package com.shujia.spark.areaRoadFlow


import com.shujia.spark.dao.factory.DAOFactory
import com.shujia.spark.util._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import com.alibaba.fastjson.{JSON, JSONObject}
import com.shujia.spark.constant.Constants
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

/** =
  * 卡扣流
  * monitor_id   1 2 3 4      1_2 2_3 3_4
  * 指定一个道路流  1 2 3 4
  * 1 carCount1 2carCount2  转化率 carCount2/carCount1
  * 1 2 3  转化率                           1 2 3的车流量/1 2的车流量
  * 1 2 3 4 转化率       1 2 3 4的车流量 / 1 2 3 的车流量
  * 京A1234	1,2,3,6,2,3
  * 1、查询出来的数据封装到cameraRDD
  * 2、计算每一车的轨迹
  * 3、匹配指定的道路流       1：carCount   1，2：carCount   1,2,3carCount
  * @author root
  */
object MonitorOneStepConvertRateAnalyzeScala {
  def main(args: Array[String]): Unit = { // 1、构造Spark上下文
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
    SparkUtils.setMaster(conf)
    val sc = new SparkContext(conf)
    val sqlContext = SparkUtilsScala.getSQLContext(sc)
    // 2、生成模拟数据
    SparkUtilsScala.mockData(sc, sqlContext)
    // 3、查询任务，获取任务的参数
    val taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT)
    val taskDAO = DAOFactory.getTaskDAO
    val task = taskDAO.findTaskById(taskid)
    if (task == null) return
    val taskParam = JSON.parseObject(task.getTaskParams)
    /**
      * 从数据库中查找出来我们指定的卡扣流
      */
    val roadFlow = ParamUtils.getParam(taskParam, Constants.PARAM_MONITOR_FLOW)
    val roadFlowBroadcast = sc.broadcast(roadFlow)
    /**
      * 通过时间的范围拿到合法的车辆
      */
    val rowRDDByDateRange = SparkUtilsScala.getCameraRDDByDateRange(sqlContext, taskParam)
    //    val collect = rowRDDByDateRange.collect
    //    println("collect.size():" + collect.length)
    /**
      * 将rowRDDByDateRange 变成key-value对的形式，key car value 详细信息
      *
      * 为什么要变成k v对的形式？
      * 因为下面要对car 按照时间排序，绘制出这辆车的轨迹。
      */
    val car2RowRDD = rowRDDByDateRange.map(row => (row.getString(3), row))
    /**
      * 计算这一辆车，有多少次匹配到咱指定的卡扣流
      *
      * 1,2,3,4,5
      *
      * 1
      * 1,2
      * 1,2,3
      * 1,2,3,4
      * 1,2,3,4,5
      *
      * 这辆车的轨迹是   1 2 3 6 7 8 1 2
      *
      * 1:2
      * 1,2 2
      * 1,2,3 1
      * 1,2,3,4 0
      * 1,2,3,4,5 0
      */
    val roadSplitRDD = generateAndMatchRowSplit(taskParam, roadFlowBroadcast, car2RowRDD)
    /**
      * roadSplitRDD
      * [1,100]
      * [1_2,100]
      * [1,200]
      * 变成了
      * [1,300]
      * [1_2,100]
      */
    val roadFlow2Count = getRoadFlowCount(roadSplitRDD)
    val convertRateMap = computeRoadSplitConvertRate(roadFlowBroadcast, roadFlow2Count)
    val entrySet = convertRateMap.entrySet

    for (entry <- entrySet) {
      System.out.println(entry.getKey + "=" + entry.getValue)
    }
  }

  private def getRoadFlowCount(roadSplitRDD: RDD[(String, Long)])  = {
    val sumByKey = roadSplitRDD.reduceByKey(_ + _).collectAsMap()
    sumByKey.toMap
  }

  private def computeRoadSplitConvertRate(roadFlowBroadcast: Broadcast[String], roadFlow2Count: Map[String, Long]) = {
    val roadFlow = roadFlowBroadcast.value
    val split = roadFlow.split(",")
    //		List<String> roadFlowList = Arrays.asList(split);
    /**
      * 存放卡扣切面的转换率
      * 1_2 0.9
      */
    val rateMap = new java.util.HashMap[String, Double]
    var lastMonitorCarCount = 0L
    for (i <- 0 until split.length) {
      val tmpRoadFlow = split.take(i + 1).mkString(",")
      val count = roadFlow2Count(tmpRoadFlow)

        /**
          * 1_2
          * lastMonitorCarCount      1 count
          */
        if (i != 0 && lastMonitorCarCount != 0L) {
          val rate = NumberUtils.formatDouble(count.toDouble / lastMonitorCarCount.toDouble, 2)
          rateMap.put(tmpRoadFlow, rate)
        }
        lastMonitorCarCount = count
    }
    rateMap.toMap
  }

  /**
    * car2RowRDD car   row详细信息
    * 按照通过时间进行排序，拿到他的轨迹
    * 1 2 3 4  1_2
    * 1 2 6 1 2 4   1_2 2
    *
    * @param taskParam
    * @param roadFlowBroadcast
    * @param car2RowRDD
    * @return
    */
  private def generateAndMatchRowSplit(taskParam: JSONObject, roadFlowBroadcast: Broadcast[String], car2RowRDD: RDD[(String, Row)]) = {

    car2RowRDD.groupByKey.flatMap(tuple => {

      /**
        * 对这个rows集合 按照车辆通过卡扣的时间排序
        */
      val list = tuple._2.toList.sortWith((r1, r2) => {
        val actionTime1 = r1.getString(4)
        val actionTime2 = r2.getString(4)
        DateUtils.before(actionTime1, actionTime2)
      })

      /**
        * roadFlowBuilder保存到是？ 卡扣id   组合起来就是这辆车的运行轨迹
        *
        * roadFlowBuilder怎么拼起来的？  rows是由顺序了，直接便利然后追加到roadFlowBuilder就可以了吧。
        */
      val roadFlow = list.map(row => row.getString(1)).mkString(",")

      /**
        * 从广播变量中获取指定的卡扣流参数
        */
      val standardRoadFlow = roadFlowBroadcast.value
      /**
        * 对指定的卡扣流参数分割
        */
      val split = standardRoadFlow.split(",")
      /**
        * 1,2,3，4,5
        * 1 2 3 4 5
        * 遍历分割完成的数组
        */
      val resultList = new java.util.ArrayList[(String, Long)]
      for (i <- 0 until split.length) {
        var tmpRoadFlow = split.take(i+1).mkString(",")
        //indexOf 从哪个位置开始查找
        var index = 0
        //这辆车有多少次匹配到这个卡扣切片的次数
        var count = 0L
        while (roadFlow.indexOf(tmpRoadFlow, index) != -1) {
          index = roadFlow.indexOf(tmpRoadFlow, index) + 1
          count += 1
        }

        resultList.add((tmpRoadFlow, count))

      }
      import scala.collection.JavaConversions._
      resultList
    })
  }
}
