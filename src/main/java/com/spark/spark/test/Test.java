package com.spark.spark.test;

import com.shujia.spark.skynet.MonitorAndCameraStateAccumulator;
import com.shujia.spark.util.DateUtils;
import com.shujia.spark.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.Iterator;
import scala.io.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by 98726 on 2017/3/9.
 */
public class Test {

    public static void main(String args[]){
        Iterator<String> lines = Source.fromFile("E:\\第一期\\大数据\\spark\\项目\\Traffic\\data\\cars.txt", "utf-8").getLines();
        while(lines.hasNext()){
            System.out.println(lines.next());
        }

    }
}
