package com.shujia.spark.areaRoadFlow

import java.util

import com.shujia.spark.conf.ConfigurationManager
import com.shujia.spark.constant.Constants
import com.shujia.spark.dao.factory.DAOFactory
import com.shujia.spark.util.{ParamUtils, SparkUtils, SparkUtilsScala}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD


/**
  * 计算出每一个区域top3的道路流量
  * 每一个区域车流量最多的3条道路   每条道路有多个卡扣
  *
  * @author root
  *         ./spark-submit  --master spark://hadoop1:7077 --executor-memory 512m  --driver-class-path /software/Hive/hive-1.2.1/lib/mysql-connector-java-5.1.26-bin.jar:/root/resource/fastjson-1.2.11.jar  --jars /software/Hive/hive-1.2.1/lib/mysql-connector-java-5.1.26-bin.jar,/root/resource/fastjson-1.2.11.jar  ~/resource/AreaTop3RoadFlowAnalyze.jar
  *
  *         这是一个分组取topN  SparkSQL分组取topN    开窗函数吧。  按照谁开窗？区域，道路流量排序，rank 小鱼等于3
  *         0
  *         区域，道路流量排序         按照区域和道路进行分组
  *         SELECT
  *         area，
  *         road_id,
  *         groupConcatDistinst(monitor_id,car) monitor_infos,
  *         count(0 car_count
  *         group by area，road_id
  *
  */
object AreaTop3RoadFlowAnalyzeScala {
  def main(args: Array[String]): Unit = { // 创建SparkConf
    val conf = new SparkConf().setAppName("AreaTop3ProductSpark")
    SparkUtils.setMaster(conf)
    // 构建Spark上下文
    val sc = new SparkContext(conf)
    val sqlContext = SparkUtilsScala.getSQLContext(sc)
    //				sqlContext.setConf("spark.sql.shuffle.partitions", "1000");
    //				sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "20971520");
    // 注册自定义函数
    sqlContext.udf.register("concat_String_string", new ConcatStringStringUDF, DataTypes.StringType)
    sqlContext.udf.register("random_prefix", new RandomPrefixUDF, DataTypes.StringType)
    sqlContext.udf.register("remove_random_prefix", new RemoveRandomPrefixUDF, DataTypes.StringType)
    sqlContext.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF)
    // 准备模拟数据
    SparkUtils.mockData(sc, sqlContext)
    // 获取命令行传入的taskid，查询对应的任务参数
    val taskDAO = DAOFactory.getTaskDAO
    val taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_TOPN_MONITOR_FLOW)
    val task = taskDAO.findTaskById(taskid)
    if (task == null) return
    System.out.println(task.getTaskParams)
    val taskParam = JSON.parseObject(task.getTaskParams)
    /**
      * 获取指定日期内的参数
      * monitor_id  car   road_id	area_id
      */
    val areaId2DetailInfos = getInfosByDateRDD(sqlContext, taskParam)
    /**
      * 从异构数据源MySQL中获取区域信息
      * area_id	area_name
      * areaId2AreaInfoRDD<area_id,row:详细信息>
      */
    val areaId2AreaInfoRDD = getAreaId2AreaInfoRDD(sqlContext)

    /**
      * 补全区域信息    添加区域名称
      * monitor_id car road_id	area_id	area_name
      * 生成基础临时信息表
      * tmp_car_flow_basic
      */
    generateTempRoadFlowBasicTable(sqlContext, areaId2DetailInfos, areaId2AreaInfoRDD)

    /**
      * 生产各个区域各个路段出现次数的临时表
      */
    generateTempAreaRoadFlowTable(sqlContext)

    /**
      * area_name
      * road_id
      * count(*) car_count
      * monitor_infos
      * 使用开窗函数  获取每一个区域的topN路段
      */
    getAreaTop3RoadFolwRDD(sqlContext)
    sc.stop()
  }

  def getAreaTop3RoadFolwRDD(sqlContext: SQLContext): RDD[Row] = {
    /**
      * tmp_area_road_flow_count表：
      * area_name
      * road_id
      * car_count
      * monitor_infos
      */
    val sql = "" + "SELECT " + "area_id," + "road_id," + "car_count," + "monitor_infos, " + "CASE " + "WHEN car_count > 170 THEN 'A LEVEL' " + "WHEN car_count > 160 AND car_count <= 170 THEN 'B LEVEL' " + "WHEN car_count > 150 AND car_count <= 160 THEN 'C LEVEL' " + "ELSE 'D LEVEL' " + "END flow_level " + "FROM (" + "SELECT " + "area_id," + "road_id," + "car_count," + "monitor_infos," + "row_number() OVER (PARTITION BY area_id ORDER BY car_count DESC) rn " + "FROM tmp_area_road_flow_count " + ") tmp " + "WHERE rn <=3"
    val df = sqlContext.sql(sql)
    df.show()
    df.rdd
  }

  def generateTempAreaRoadFlowTable(sqlContext: SQLContext): Unit = {
    /**
      * 	structFields.add(DataTypes.createStructField("area_id", DataTypes.StringType, true));
      *structFields.add(DataTypes.createStructField("area_name", DataTypes.StringType, true));
      *structFields.add(DataTypes.createStructField("road_id", DataTypes.StringType, true));
      *structFields.add(DataTypes.createStructField("monitor_id", DataTypes.StringType, true));
      *structFields.add(DataTypes.createStructField("car", DataTypes.StringType, true));
      */
    val sql = "SELECT " + "area_id," + "road_id," + "count(*) car_count," + "group_concat_distinct(monitor_id) monitor_infos " + "FROM tmp_car_flow_basic " + "GROUP BY area_id,road_id"
    val sqlText = "" + "SELECT " + "area_name_road_id," + "sum(car_count)," + "group_concat_distinct(monitor_infos) monitor_infoss " + "FROM (" + "SELECT " + "remove_random_prefix(area_name_road_id) area_name_road_id," + "car_count," + "monitor_infos " + "FROM (" + "SELECT " + "area_name_road_id," + "count(*) car_count," + "group_concat_distinct(monitor_id) monitor_infos " + "FROM (" + "SELECT " + "monitor_id," + "car," + "random_prefix(concat_String_string(area_name,road_id,':'),10) area_name_road_id " + "FROM tmp_car_flow_basic " + ") t1 " + "GROUP BY area_name_road_id " + ") t2 " + ") t3 " + "GROUP BY area_name_road_id"
    val df = sqlContext.sql(sql)
    //		df.show();
    df.registerTempTable("tmp_area_road_flow_count")
  }

  def generateTempRoadFlowBasicTable(sqlContext: SQLContext, areaId2DetailInfos: RDD[(String, Row)], areaId2AreaInfoRDD: RDD[(String, Row)]): Unit = {
    val tmpRowRDD = areaId2DetailInfos.join(areaId2AreaInfoRDD).map(tuple => {
      val areaId = tuple._1
      val carFlowDetailRow = tuple._2._1
      val areaDetailRow = tuple._2._2
      val monitorId = carFlowDetailRow.getString(0)
      val car = carFlowDetailRow.getString(1)
      val roadId = carFlowDetailRow.getString(2)
      val areaName = areaDetailRow.getString(1)
      RowFactory.create(areaId, areaName, roadId, monitorId, car)
    })
    val structFields = new util.ArrayList[StructField]
    structFields.add(DataTypes.createStructField("area_id", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("area_name", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("road_id", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("monitor_id", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("car", DataTypes.StringType, true))
    val schema = DataTypes.createStructType(structFields)
    val df = sqlContext.createDataFrame(tmpRowRDD, schema)
    df.registerTempTable("tmp_car_flow_basic")
  }

  private def getAreaId2AreaInfoRDD(sqlContext: SQLContext) = {
    var url = ""
    var user = ""
    var password = ""
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL)
      user = ConfigurationManager.getProperty(Constants.JDBC_USER)
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
    }
    else {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD)
      user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD)
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD)
    }
    val options = new util.HashMap[String, String]
    options.put("url", url)
    options.put("dbtable", "area_info")
    options.put("user", user)
    options.put("password", password)
    // 通过SQLContext去从MySQL中查询数据
    val areaInfoDF = sqlContext.read.format("jdbc").options(options).load
    // 返回RDD
    val areaInfoRDD = areaInfoDF.rdd
    val areaid2areaInfoRDD = areaInfoRDD.map(row => {
      val areaid = row.getString(0)
      (areaid, row)
    })
    areaid2areaInfoRDD
  }

  private def getInfosByDateRDD(sqlContext: SQLContext, taskParam: JSONObject): RDD[(String, Row)] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "SELECT " + "monitor_id," + "car," + "road_id," + "area_id " + "FROM	monitor_flow_action " + "WHERE date >= '" + startDate + "'" + "AND date <= '" + endDate + "'"
    val df = sqlContext.sql(sql)
    df.rdd.map(row => {
      val areaId = row.getString(3)
      (areaId, row)
    })
  }
}
