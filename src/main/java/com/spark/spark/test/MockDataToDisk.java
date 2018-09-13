package com.spark.spark.test;
import com.shujia.spark.util.DateUtils;
import com.shujia.spark.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.Iterator;
import scala.io.Source;

import java.util.*;
import java.util.Map.Entry;


/**
 * 模拟数据  数据格式如下：
 * 卡口ID	monitor_id	车牌号	拍摄时间	车速	通道ID
 *
 * @author Administrator
 * a
 */
public class MockDataToDisk {
    /**
     * date	卡口ID		camera_id	车牌号	拍摄时间	车速	道路ID 区域ID
     *
     */
    public static void main(String [] args) {
        List<String> dataList = new ArrayList<String>();
        Random random = new Random();

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("app"));

        String[] locations = new String[]{"鲁", "京", "京", "京", "沪", "京", "京", "深", "京", "京"};
//     	String[] areas = new String[]{"海淀区","朝阳区","昌平区","东城区","西城区","丰台区","顺义区","大兴区"};
        String date = DateUtils.getTodayDate();

//    	StringBuilder carStringBuilder = new StringBuilder();
        Iterator<String> cars = Source.fromFile("E:\\第一期\\大数据\\spark\\项目\\Traffic\\data\\cars.txt", "utf-8").getLines();
        while(cars.hasNext()){
            String car = cars.next();
            String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24) + "");
            for (int j = 0; j < random.nextInt(300); j++) {
                if (j % 30 == 0 && j != 0) {
                    baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1]) + 1) + "");
                }
                String actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60) + "") + ":" + StringUtils.fulfuill(random.nextInt(60) + "");
                String monitorId = StringUtils.fulfuill(4, random.nextInt(9) + "");
                String speed = random.nextInt(260) + "";
                String roadId = random.nextInt(50) + 1 + "";
                String cameraId = StringUtils.fulfuill(5, random.nextInt(9999) + "");
                String areaId = StringUtils.fulfuill(2, random.nextInt(8) + "");
                String row = date + "\t" + monitorId + "\t" + cameraId + "\t" + car + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId;
                //Row row = RowFactory.create(date, monitorId, cameraId, car, actionTime, speed, roadId, areaId);
                dataList.add(row);
            }
        }
        sc.parallelize(dataList).saveAsTextFile("E:\\第一期\\大数据\\spark\\项目\\Traffic\\data\\monitor_flow_action");


        /**
         * 将集合清空  模拟出每一个卡口所对应的摄像头的编号
         * 数据格式如下：
         * 			monitor_id	camera_id
         */
        /**
         * key  卡扣ID
         * value   这个卡扣下所有的cameraid
         */
        Map<String, Set<String>> monitorAndCameras = new HashMap<>();


        int index = 0;
        for (String row : dataList) {
            String[] split = row.split("\t");
            Set<String> sets = monitorAndCameras.get(split[1]);
            if (sets == null) {
                sets = new HashSet<>();
                monitorAndCameras.put((String) split[1], sets);
            }
            index++;
            if (index % 1000 == 0) {
                sets.add(StringUtils.fulfuill(5, random.nextInt(99999) + ""));
            }
            sets.add(split[2]);
        }

        dataList.clear();

        Set<Entry<String, Set<String>>> entrySet = monitorAndCameras.entrySet();
        for (Entry<String, Set<String>> entry : entrySet) {
            Set<String> sets = entry.getValue();
            String row = null;
            for (String val : sets) {
//                row = RowFactory.create(entry.getKey(), val);
                row = entry.getKey() + "\t" + val;
                dataList.add(row);
            }
        }
       sc.parallelize(dataList).saveAsTextFile("E:\\第一期\\大数据\\spark\\项目\\Traffic\\data\\monitor_camera_info");

    }
}














