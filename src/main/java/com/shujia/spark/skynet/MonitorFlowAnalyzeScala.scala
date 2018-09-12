//package com.shujia.spark.skynet
//
//import java.util
//import java.util.{ArrayList, Arrays, HashMap, Iterator, List, Map}
//
//import com.shujia.spark.constant.Constants
//import com.shujia.spark.dao.IAreaDao
//import com.shujia.spark.dao.IMonitorDAO
//import com.shujia.spark.dao.ITaskDAO
//import com.shujia.spark.dao.factory.DAOFactory
//import com.shujia.spark.util.{ParamUtils, SparkUtils, SparkUtilsScala, StringUtils}
//import org.apache.spark.{Accumulator, SparkConf, SparkContext}
//import org.apache.spark.api.java.JavaPairRDD
//import org.apache.spark.api.java.JavaRDD
//import org.apache.spark.api.java.JavaSparkContext
//import org.apache.spark.api.java.function.Function
//import org.apache.spark.api.java.function.PairFlatMapFunction
//import org.apache.spark.api.java.function.PairFunction
//import org.apache.spark.api.java.function.VoidFunction
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.SQLContext
//import com.alibaba.fastjson.JSONObject
//import com.alibaba.fastjson.JSON
//import com.shujia.spark.domain.Area
//import com.shujia.spark.domain.MonitorState
//import com.shujia.spark.domain.Task
//import com.shujia.spark.domain.TopNMonitor2CarCount
//import com.shujia.spark.domain.TopNMonitorDetailInfo
//import com.google.common.base.Optional
//import org.apache.spark.rdd.RDD
//
//import scala.Tuple2
//
//
///**
//  * 卡口流量监控模块
//  * 1、卡口数量的正常数量，异常数量，还有通道数同时查询出车流量排名前N的卡口  持久化到数据库中
//  * 2、根据指定的卡口号和查询日期查询出此时的卡口的流量信息
//  * 3、基于2功能点的基础上多维度搜车，通过车牌颜色，车辆类型，车辆颜色，车辆品牌，车辆型号，车辆年款信息进行多维度搜索，这个功能点里面会使用到异构数据源。
//  *
//  * @author root
//  *
//  */
//object MonitorFlowAnalyzeScala {
//  def main(args: Array[String]): Unit = { // 构建Spark运行时的环境参数
//    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION) //						.set("spark.sql.shuffle.partitions", "10")
//    //						.set("spark.default.parallelism", "100")
//    //						.set("spark.storage.memoryFraction", "0.5")
//    //						.set("spark.shuffle.consolidateFiles", "true")
//    //						.set("spark.shuffle.file.buffer", "64")
//    //						.set("spark.shuffle.memoryFraction", "0.3")
//    //						.set("spark.reducer.maxSizeInFlight", "24")
//    //						.set("spark.shuffle.io.maxRetries", "60")
//    //						.set("spark.shuffle.io.retryWait", "60")
//    //						.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    //						.registerKryoClasses(new Class[]{
//    //								SpeedSortKey.class})
//
//    /**
//      * 设置spark运行时的master  根据配置文件来决定的
//      */
//    SparkUtils.setMaster(conf)
//    val sc = new SparkContext(conf)
//    val sqlContext = SparkUtilsScala.getSQLContext(sc)
//
//    /**
//      * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的临时表就可以
//      * 本地模拟数据注册成一张临时表
//      * monitor_flow_action
//      */
//    SparkUtils.mockData(sc, sqlContext)
//    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR)
//    val taskDAO = DAOFactory.getTaskDAO
//    val task = taskDAO.findTaskById(taskId)
//    if (task == null) return
//    val taskParamsJsonObject = JSON.parseObject(task.getTaskParams)
//    var cameraRDD = SparkUtilsScala.getCameraRDDByDateRange(sqlContext, taskParamsJsonObject)
//    cameraRDD = cameraRDD.cache
//    val monitorAndCameraStateAccumulator = sc.accumulator("")(new MonitorAndCameraStateAccumulator)
//
//    /**
//      * 将cameraRDD变成kv格式的rdd
//      */
//    var monitor2DetailRDD = getMonitor2DetailRDD(cameraRDD)
//    monitor2DetailRDD = monitor2DetailRDD.cache
//    /**
//      * 以monitor_Id为单位进行聚合，如何以monitor_id作为单位的？
//      * groupByKey
//      * monitor_id,infos(area_id,monitor_id,camera_count,camera_ids,car_count)
//      */
//    val aggregateMonitorId2DetailRDD = aggreagteByMonitor(monitor2DetailRDD)
//    val carCount2MonitorRDD = checkMonitorState(sc, sqlContext, aggregateMonitorId2DetailRDD, taskId, taskParamsJsonObject, monitorAndCameraStateAccumulator)
//    //获取卡口流量的前N名，并且持久化到数据库中
//    //topNMonitor2CarFlow  key:monitor_id  v:monitor_id
//    val topNMonitor2CarFlow = getTopNMonitorCarFlow(sc, taskId, taskParamsJsonObject, carCount2MonitorRDD)
//    saveMonitorState(taskId, monitorAndCameraStateAccumulator)
//    //获取topN 卡口的具体信息
//    getTopNDetails(taskId, topNMonitor2CarFlow, monitor2DetailRDD)
//    val top50MonitorIds = speedTopNMonitor(monitor2DetailRDD)
//    import scala.collection.JavaConversions._
//    for (monitorId <- top50MonitorIds) {
//      System.out.println("monitorId:" + monitorId)
//    }
//
//    /**
//      * 高速通过的top50的monitorId
//      * 每一个monitorId下面再找出来通过速度最快的top10
//      * 一共会有50 *１０　＝　５００　
//      * 1、根据top50这个列表，过滤
//      * 2、根据过滤出来的结果  分组 groupByKey（monitorId）
//      * 3、组内排序（
//      * 注意：尽量避免使用集合类。为什么？
//      * 有可能这一个monitorId下面所对应的数据量非常的大，OOM
//      * ），然后取出来topN
//      *
//      * monitor2DetailRDD
//      * key:monitor_id
//      * value:row
//      */
//    getMonitorDetails(sc, taskId, top50MonitorIds, monitor2DetailRDD)
//    sc.stop()
//  }
//
//  private def getAreaInfosFromDB = {
//    val areaDao = DAOFactory.getAreaDao
//    val findAreaInfo = areaDao.findAreaInfo
//    val areaMap = new util.HashMap[String, String]
//    import scala.collection.JavaConversions._
//    for (area <- findAreaInfo) {
//      areaMap.put(area.getAreaId, area.getAreaName)
//    }
//    areaMap
//  }
//
//  /**
//    * 50个卡扣   50个卡扣下  速度top10的车辆         500条记录？       分组取top10   groupByKey按照key进行了分组，然后对组内的数据进行倒叙排序，然后取top10
//    *
//    * @param sc
//    * @param taskId
//    * @param top10MonitorIds
//    * @param monitor2DetailRDD
//    */
//  private def getMonitorDetails(sc: SparkContext, taskId: Long, top10MonitorIds: util.List[String], monitor2DetailRDD: RDD[String, Row]): Unit = ???
//
//  /**
//    * 1、每一辆车都有speed    按照速度划分是否是高速 中速 普通 低速
//    * 2、每一辆车的车速都在一个车速段     对每一个卡扣进行聚合   拿到高速通过 中速通过  普通  低速通过的车辆各是多少辆
//    * 3、四次排序   先按照高速通过车辆数   中速通过车辆数   普通通过车辆数   低速通过车辆数
//    *
//    * @param cameraRDD
//    * @return
//    */
//  private def speedTopNMonitor(monitorId2DetailRDD: RDD[(String, Row)]) = {
//    val groupByMonitorId = monitorId2DetailRDD.groupByKey
//    /**
//      * key:自定义的类  value：卡扣ID
//      */
//    val speedSortKey2MonitorId = groupByMonitorId.map(tuple=>{
//      val monitorId = tuple._1
//      val speedIterator = tuple._2.iterator
//      var lowSpeed = 0
//      var normalSpeed = 0
//      var mediumSpeed = 0
//      var highSpeed = 0
//      while ( {
//        speedIterator.hasNext
//      }) {
//        val speed = StringUtils.convertStringtoInt(speedIterator.next.getString(5))
//        if (speed >= 0 && speed < 60) lowSpeed += 1
//        else if (speed >= 60 && speed < 90) normalSpeed += 1
//        else if (speed >= 90 && speed < 120) mediumSpeed += 1
//        else if (speed >= 120) highSpeed += 1
//      }
//      val speedSortKey = new SpeedSortKey(lowSpeed, normalSpeed, mediumSpeed, highSpeed)
//      new Tuple2[SpeedSortKey, String](speedSortKey, monitorId)
//    })
//    val sortBySpeedCount = speedSortKey2MonitorId.sortByKey(false)
//    val take = sortBySpeedCount.take(50)
//    /**
//      * 最畅通的top50
//      * 这个list里面有50条记录
//      */
//    val monitorIds = new util.ArrayList[String]
//    import scala.collection.JavaConversions._
//    for (tuple <- take) {
//      monitorIds.add(tuple._2)
//    }
//    monitorIds
//  }
//
//  /**
//    * 按照monitor_进行聚合，cameraId camer_count
//    *
//    * @param monitorId2Detail
//    * @return
//    */
//  private def aggreagteByMonitor(monitorId2Detail: RDD[(String, Row)]) = {
//    /**
//      * <monitor_id,List<Row> 集合里面的记录代表的是camera的信息。>
//      */
//    val monitorId2RowRDD = monitorId2Detail.groupByKey
//    /**
//      * 一个monitor_id对应一条记录
//      * 为什么使用mapToPair来遍历数据，因为我们要操作的返回值是每一个monitorid 所对应的详细信息
//      */
//    val monitorId2CameraCountRDD = monitorId2RowRDD.map(tuple=>{
//      val monitorId = tuple._1
//      val rowIterator = tuple._2.iterator
//      val list = new util.ArrayList[String]
//      val tmpInfos = new StringBuilder
//      var count = 0
//      var areaId = ""
//
//      /**
//        * 这个while循环  代表的是这个卡扣一共经过了多少辆车   一辆车的信息就是一个row
//        */
//      while ( {
//        rowIterator.hasNext
//      }) {
//        val row = rowIterator.next
//        areaId = row.getString(7)
//        val cameraId = row.getString(2)
//        if (!list.contains(cameraId)) list.add(cameraId)
//        if (!tmpInfos.toString.contains(cameraId)) tmpInfos.append("," + row.getString(2))
//        count += 1
//      }
//      /**
//        * camera的个数
//        */
//      val cameraCount = list.size
//      val infos = Constants.FIELD_MONITOR_ID + "=" + monitorId + "|" + Constants.FIELD_AREA_ID + "=" + areaId + "|" + Constants.FIELD_CAMERA_IDS + "=" + tmpInfos.toString.substring(1) + "|" + Constants.FIELD_CAMERA_COUNT + "=" + cameraCount + "|" + Constants.FIELD_CAR_COUNT + "=" + count
//      new Tuple2[String, String](monitorId, infos)
//    })
//    //<monitor_id,camera_infos(ids,cameracount,carCount)>
//    monitorId2CameraCountRDD
//  }
//
//  private def saveMonitorState(taskId: Long, monitorAndCameraStateAccumulator: Accumulator[String]): Unit = {
//    /**
//      * 累加器中值能在Executor段读取吗？
//      * 不能
//      * 这里的读取时在Driver中进行的
//      */
//    val accumulatorVal = monitorAndCameraStateAccumulator.value
//    val normalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_MONITOR_COUNT)
//    val normalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_CAMERA_COUNT)
//    val abnormalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_COUNT)
//    val abnormalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_CAMERA_COUNT)
//    val abnormalMonitorCameraInfos = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS)
//    /**
//      * 这里面只有一条记录
//      */
//    val monitorState = new MonitorState(taskId, normalMonitorCount, normalCameraCount, abnormalMonitorCount, abnormalCameraCount, abnormalMonitorCameraInfos)
//    val monitorDAO = DAOFactory.getMonitorDAO
//    monitorDAO.insertMonitorState(monitorState)
//  }
//
//  private def getMonitor2DetailRDD(cameraRDD: JavaRDD[Row]) = {
//    val monitorId2Detail = cameraRDD.mapToPair(new PairFunction[Row, String, Row]() {
//      @throws[Exception]
//      override def call(row: Row) = new Tuple2[String, Row](row.getString(1), row)
//    })
//    monitorId2Detail
//  }
//
//  private def getTopNDetails(taskId: Long, topNMonitor2CarFlow: JavaPairRDD[String, String], monitor2DetailRDD: JavaPairRDD[String, Row]): Unit = {
//    /**
//      * 获取车流量排名前N的卡口的详细信息   可以看一下是在什么时间段内卡口流量暴增的。
//      */
//    topNMonitor2CarFlow.join(monitor2DetailRDD).mapToPair(new PairFunction[Tuple2[String, Tuple2[String, Row]], String, Row]() {
//      @throws[Exception]
//      override def call(t: Tuple2[String, Tuple2[String, Row]]) = new Tuple2[String, Row](t._1, t._2._2)
//    }).foreachPartition(new VoidFunction[util.Iterator[Tuple2[String, Row]]]() {
//      @throws[Exception]
//      override def call(t: util.Iterator[Tuple2[String, Row]]): Unit = {
//        val monitorDetailInfos = new util.ArrayList[TopNMonitorDetailInfo]
//        while ( {
//          t.hasNext
//        }) {
//          val tuple = t.next
//          val row = tuple._2
//          val m = new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6))
//          monitorDetailInfos.add(m)
//        }
//        val monitorDAO = DAOFactory.getMonitorDAO
//        monitorDAO.insertBatchMonitorDetails(monitorDetailInfos)
//      }
//    })
//
//  }
//
//  /**
//    * 获取卡口流量的前三名，并且持久化到数据库中
//    *
//    * @param taskId
//    * @param taskParamsJsonObject
//    * @param carCount2MonitorId
//    */
//  private def getTopNMonitorCarFlow(sc: JavaSparkContext, taskId: Long, taskParamsJsonObject: JSONObject, carCount2MonitorId: JavaPairRDD[Integer, String]) = {
//    /**
//      * 获取车流量排名前三的卡口信息
//      * 有什么作用？ 当某一个卡口的流量这几天突然暴增和往常的流量不相符，交管部门应该找一下原因，是什么问题导致的，应该到现场去疏导车辆。
//      */
//    val topNumFromParams = ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_TOP_NUM).toInt
//    /**
//      * carCount2MonitorId <carCount,monitor_id>
//      */
//    val topNCarCount = carCount2MonitorId.sortByKey(false).take(topNumFromParams)
//    val topNMonitor2CarCounts = new util.ArrayList[TopNMonitor2CarCount]
//    import scala.collection.JavaConversions._
//    for (tuple <- topNCarCount) {
//      val topNMonitor2CarCount = new TopNMonitor2CarCount(taskId, tuple._2, tuple._1)
//      topNMonitor2CarCounts.add(topNMonitor2CarCount)
//    }
//    val ITopNMonitor2CarCountDAO = DAOFactory.getMonitorDAO
//    ITopNMonitor2CarCountDAO.insertBatchTopN(topNMonitor2CarCounts)
//    val monitorId2CarCounts = new util.ArrayList[Tuple2[String, String]]
//    import scala.collection.JavaConversions._
//    for (t <- topNCarCount) {
//      monitorId2CarCounts.add(new Tuple2[String, String](t._2, t._2))
//    }
//    val monitorId2CarCountRDD = sc.parallelizePairs(monitorId2CarCounts)
//    monitorId2CarCountRDD
//  }
//
//  /**
//    * 检测卡口状态
//    *
//    * @param sc
//    */
//  private def checkMonitorState(sc: SparkContext, sqlContext: SQLContext, monitorId2CameraCountRDD: RDD[(String, String)], taskId: Long, taskParamsJsonObject: JSONObject, monitorAndCameraStateAccumulator: Accumulator[String]) = {
//    /**
//      * 从monitor_camera_info表中查询出来每一个卡口对应的camera的数量
//      */
//    val sqlText = "" + "SELECT * " + "FROM monitor_camera_info"
//    val standardDF = sqlContext.sql(sqlText)
//    val standardRDD = standardDF.rdd
//    val monitorId2CameraId = standardRDD.map(x => (x.getString(0), x.getString(1))
//    val standardDonitor2CameraInfos = monitorId2CameraId.groupByKey().map(tuple => {
//      val monitorId = tuple._1
//      val cameraIterator = tuple._2.iterator
//      var count = 0
//      val cameraIds = new StringBuilder
//      while ( {
//        cameraIterator.hasNext
//      }) {
//        cameraIds.append("," + cameraIterator.next)
//        count += 1
//      }
//      val cameraInfos = Constants.FIELD_CAMERA_IDS + "=" + cameraIds.toString.substring(1) + "|" + Constants.FIELD_CAMERA_COUNT + "=" + count
//      (monitorId, cameraInfos)
//    })
//    /**
//      * rdd1:1	198
//      * rdd2:2	987
//      * 1	765
//      *
//      * 1 	198	null
//      * 1	198	765
//      */
//    val joinResultRDD = standardDonitor2CameraInfos.leftOuterJoin(monitorId2CameraCountRDD)
//    joinResultRDD
//  }
//}
//
