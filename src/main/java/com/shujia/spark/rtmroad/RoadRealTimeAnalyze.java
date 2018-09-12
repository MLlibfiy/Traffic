package com.shujia.spark.rtmroad;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.shujia.spark.conf.ConfigurationManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.shujia.spark.constant.Constants;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class RoadRealTimeAnalyze {
	public static void main(String[] args) {
		// 构建Spark Streaming上下文
				SparkConf conf = new SparkConf()       
 		 				.setMaster("local[2]")
						.setAppName("AdClickRealTimeStatSpark");
//						.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//						.set("spark.default.parallelism", "1000");
//						.set("spark.streaming.blockInterval", "50");    
//						.set("spark.streaming.receiver.writeAheadLog.enable", "true");   
				
				JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
				
				
 				jssc.checkpoint("E:\\第一期\\大数据\\spark\\项目\\Traffic\\out\\streaming_checkpoint");
				
				Map<String, String> kafkaParams = new HashMap<String, String>();
				String brokers = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST);
				kafkaParams.put("metadata.broker.list",brokers);
				
				// 构建topic set
				String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
				String[] kafkaTopicsSplited = kafkaTopics.split(",");  
				
				Set<String> topics = new HashSet<String>();
				for(String kafkaTopic : kafkaTopicsSplited) {
					topics.add(kafkaTopic);
				}
				
				JavaPairInputDStream<String, String> carRealTimeLogDStream = KafkaUtils.createDirectStream(
						jssc, 
						String.class, 
						String.class, 
						StringDecoder.class, 
						StringDecoder.class, 
						kafkaParams, 
						topics);
				
				/**
				 * 实时计算道路的拥堵情况
				 */
				realTimeCalculateRoadState(carRealTimeLogDStream);
				
				String path = "d:\\temp\\spark\\ControlCar.txt";
				List<String> readFile = readFile(path);
				for (String string : readFile) {
					System.out.println(string);

				}
				/**
				 * 缉查布控，在各个卡扣中布控某一批嫌疑车辆的车牌
				 */

  				JavaDStream<String> resultDStream = controlCars(path,carRealTimeLogDStream);
  				resultDStream.print();
//
				jssc.start();
				jssc.awaitTermination();
	}

 


	private static JavaDStream<String> controlCars(final String path, JavaPairInputDStream<String, String> carRealTimeLogDStream) {
		return carRealTimeLogDStream.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@SuppressWarnings("resource")
			@Override
			public JavaRDD<String> call(JavaPairRDD<String, String> rdd) throws Exception {
				
				List<String> carList = readFile(path);
				JavaSparkContext sc = new JavaSparkContext(rdd.context());
				final Broadcast<List<String>> broadcast = sc.broadcast(carList);
				System.out.println("broadcast.value()"+broadcast.value());
				return rdd.filter(new Function<Tuple2<String,String>, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, String> v1) throws Exception {
						/**
						 * 
						 */
						String log = v1._2;
						String car = log.split("\t")[3];

						List<String> value = broadcast.value();
						if(value.contains(car)){
							return true;
						}else{
							return false;
						}
					}
				}).map(new Function<Tuple2<String,String>, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> v1) throws Exception {
						return v1._2;
					}
				});
			}
		});
	}




	@SuppressWarnings("resource")
	public static List<String> readFile(String path){
		try {
			BufferedReader br;
				br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
			List<String> controlList = new ArrayList<>();
			String readLine = "";
			while((readLine = br.readLine())!=null){
				controlList.add(new String(readLine.getBytes(),"UTF-8"));
			}
			return controlList;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private static void realTimeCalculateRoadState(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		JavaDStream<String> roadRealTimeLog = adRealTimeLogDStream.map(new Function<Tuple2<String,String>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> tuple) throws Exception {
				return tuple._2;
			}
		});
		
		/**
		 * 拿到车辆的信息了
		 * 		car speed monitorId
		 * <Monitor_id,Speed>
		 */
		
		 JavaPairDStream<String, Tuple2<Integer, Integer>> monitorId2SpeedDStream = roadRealTimeLog.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String log) throws Exception {
				String[] split = log.split("\t");
				return new Tuple2<String, Integer>(split[1], Integer.parseInt(split[5]));
			}
		}).mapValues(new Function<Integer, Tuple2<Integer,Integer>>() {
			
			/**
			 * <monitorId	<speed,1>>
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Integer v1) throws Exception {
				return new Tuple2<Integer, Integer>(v1, 1);
			}
		});
		 
		 JavaPairDStream<String, Tuple2<Integer, Integer>> resultDStream = monitorId2SpeedDStream.reduceByKeyAndWindow(new Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
				return new Tuple2<Integer, Integer>(v1._1+v2._1, v1._2+v2._2);
			}
		}, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
				
				return new Tuple2<Integer, Integer>(v1._1 - v2._1,v2._2 - v2._2);
			}
		}, Durations.minutes(1), Durations.seconds(10));
		 
		 
		 /**
		  * 使用reduceByKeyAndWindow  窗口大小是1分钟，为什么不讲batch Interval设置为一分钟呢？
		  * 	1、batch Interval  1m   每隔1m才会计算一次。 这无法改变，如果使用reduceByKeyAndWindow 但是呢batch interval 5s，每隔多少秒计算，可以自己来指定。
		  * 	2、如果你的application还有其他的功能点的话，另外一个功能点不能忍受这个长的延迟。权衡一下还是使用reduceByKeyAndWindow。
		  */
		
		 resultDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<Integer, Integer>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Tuple2<Integer, Integer>> rdd) throws Exception {
				final SimpleDateFormat secondFormate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				
				
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<Integer,Integer>>>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Tuple2<Integer, Integer>>> iterator) throws Exception {
						while (iterator.hasNext()) {
							Tuple2<String, Tuple2<Integer, Integer>> tuple = iterator.next();
							String monitor = tuple._1;
							int speedCount = tuple._2._1;
							int carCount = tuple._2._2;
							
							System.out.println("时间："+secondFormate.format(Calendar.getInstance().getTime())+"卡扣编号："+monitor + "车辆总数："+carCount + "速度总数：" + speedCount);
						}
					}
				});
			}
		});
		
	}
}
