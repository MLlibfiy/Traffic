package com.spark.spark.test;

import com.shujia.spark.skynet.MonitorAndCameraStateAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by 98726 on 2017/3/9.
 */
public class Test {

    public static void main(String args[]){
        SparkContext sc= new SparkContext(new SparkConf());

        sc.accumulator("",new MonitorAndCameraStateAccumulator());
    }
}
