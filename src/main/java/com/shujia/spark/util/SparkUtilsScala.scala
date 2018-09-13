package com.shujia.spark.util

import java.util
import java.util.Arrays

import com.shujia.spark.conf.ConfigurationManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import com.alibaba.fastjson.JSONObject
import com.shujia.spark.constant.Constants
import com.spark.spark.test.MockData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructType}


/**
  * Spark工具类
  *
  * @author Administrator
  *
  */
object SparkUtilsScala {
  /**
    * 根据当前是否本地测试的配置
    * 决定，如何设置SparkConf的master
    */
  def setMaster(conf: SparkConf): Unit = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) conf.setMaster("local[1]")
  }

  /**
    * 获取SQLContext
    * 如果spark.local设置为true，那么就创建SQLContext；否则，创建HiveContext
    *
    * @param sc
    * @return
    */
  def getSQLContext(sc: SparkContext): SQLContext = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local){
      new HiveContext(sc)
    } else new HiveContext(sc)
  }

  /**
    * 生成模拟数据
    * 如果spark.local配置设置为true，则生成模拟数据；否则不生成
    *
    * @param sc
    * @param sqlContext
    */
  def mockData(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)

    /**
      * 如何local为true  说明在本地测试  应该生产模拟数据    RDD-》DataFrame-->注册成临时表0
      * false    HiveContext  直接可以操作hive表
      */
    if (local) {
      val monitorFlowActionRDD = sc.textFile("E:\\第一期\\大数据\\spark\\项目\\Traffic\\data\\monitor_flow_action")

      val monitorFlowActionRowRDD = monitorFlowActionRDD
        .map(_.split("\t"))
        .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))

      val cameraFlow = util.Arrays.asList(
        DataTypes.createStructField("date", DataTypes.StringType, true),
        DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
        DataTypes.createStructField("camera_id", DataTypes.StringType, true),
        DataTypes.createStructField("car", DataTypes.StringType, true),
        DataTypes.createStructField("action_time", DataTypes.StringType, true),
        DataTypes.createStructField("speed", DataTypes.StringType, true),
        DataTypes.createStructField("road_id", DataTypes.StringType, true),
        DataTypes.createStructField("area_id", DataTypes.StringType, true))

      val cameraFlowSchema = DataTypes.createStructType(cameraFlow)

      val monitorFlowActionDF = sqlContext.createDataFrame(monitorFlowActionRowRDD, cameraFlowSchema)
      monitorFlowActionDF.show()
      monitorFlowActionDF.registerTempTable("monitor_flow_action")


      val monitorCameraInfoRDD = sc.textFile("E:\\第一期\\大数据\\spark\\项目\\Traffic\\data\\monitor_camera_info")
      val monitorCameraInfoRowRDD = monitorCameraInfoRDD
        .map(_.split("\t"))
        .map(x => Row(x(0), x(1)))
      val monitorSchema = DataTypes.createStructType(util.Arrays.asList(
        DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
        DataTypes.createStructField("camera_id", DataTypes.StringType, true)))

      val monitorCameraInfoDF = sqlContext.createDataFrame(monitorCameraInfoRowRDD, monitorSchema)
      monitorCameraInfoDF.show()
      monitorCameraInfoDF.registerTempTable("monitor_camera_info")

    }
  }

  /**
    * 获取指定日期范围内的卡口信息
    *
    * @param sqlContext
    * @param taskParamsJsonObject
    */
  def getCameraRDDByDateRange(sqlContext: SQLContext, taskParamsJsonObject: JSONObject): RDD[Row] = {
    val startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE)
    val sql = "SELECT * FROM monitor_flow_action " + "WHERE date>='" + startDate + "' " + "AND date<='" + endDate + "'"
    val monitorDF = sqlContext.sql(sql)

    /**
      * repartition可以提高stage的并行度
      */
    //		return actionDF.javaRDD().repartition(1000);
    monitorDF.rdd
  }

  def getCameraRDDByDateRangeAndCars(sqlContext: SQLContext, taskParamsJsonObject: JSONObject): RDD[Row] = {
    val startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE)
    val cars = ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_CARS)
    println(startDate + "\t" + endDate)
    var sql = "SELECT * " + "FROM monitor_flow_action " + "WHERE" +
      " date>='" + startDate + "' " +
      "AND date<='" + endDate + "' " +
      "AND car IN ('"+cars.split(",").mkString("','")+"')"
    println("sql:" + sql)
    val monitorDF = sqlContext.sql(sql)
    monitorDF.rdd
  }
}
