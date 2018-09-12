package com.shujia.spark.util

import com.shujia.spark.conf.ConfigurationManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.alibaba.fastjson.JSONObject
import com.shujia.spark.constant.Constants
import com.spark.spark.test.MockData
import org.apache.spark.rdd.RDD


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
    if (local) conf.setMaster("local")
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
    if (local) new HiveContext(sc)
    else new HiveContext(sc)
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
    if (local) MockData.mock(sc, sqlContext)
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
    val sql = "SELECT * " + "FROM monitor_flow_action " + "WHERE date>='" + startDate + "' " + "AND date<='" + endDate + "'"
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
    val carArr = cars.split(",")
    var sql = "SELECT * " + "FROM monitor_flow_action " + "WHERE date>='" + startDate + "' " + "AND date<='" + endDate + "' " + "AND car IN ("
    var i = 0
    while ( {
      i < carArr.length
    }) {
      sql += "'" + carArr(i) + "'"
      if (i < carArr.length - 1) sql += ","

      {
        i += 1; i - 1
      }
    }
    sql += ")"
    System.out.println("sql:" + sql)
    val monitorDF = sqlContext.sql(sql)
    monitorDF.rdd
  }
}
