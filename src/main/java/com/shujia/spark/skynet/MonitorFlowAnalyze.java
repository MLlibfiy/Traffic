package com.shujia.spark.skynet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.shujia.spark.constant.Constants;
import com.shujia.spark.dao.IAreaDao;
import com.shujia.spark.dao.IMonitorDAO;
import com.shujia.spark.dao.ITaskDAO;
import com.shujia.spark.dao.factory.DAOFactory;
import com.shujia.spark.util.ParamUtils;
import com.shujia.spark.util.SparkUtils;
import com.shujia.spark.util.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.shujia.spark.domain.Area;
import com.shujia.spark.domain.MonitorState;
import com.shujia.spark.domain.Task;
import com.shujia.spark.domain.TopNMonitor2CarCount;
import com.shujia.spark.domain.TopNMonitorDetailInfo;
import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * 卡口流量监控模块
 * 1、卡口数量的正常数量，异常数量，
 *
 * 还有通道数同时查询出车流量排名前N的卡口  持久化到数据库中
 * 2、根据指定的卡口号和查询日期查询出此时的卡口的流量信息
 * 3、基于2功能点的基础上多维度搜车，通过车牌颜色，车辆类型，车辆颜色，车辆品牌，车辆型号，车辆年款信息进行多维度搜索，这个功能点里面会使用到异构数据源。
 *
 * @author root
 */
public class MonitorFlowAnalyze {
    public static void main(String[] args) {
        // 构建Spark运行时的环境参数
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
//						.set("spark.sql.shuffle.partitions", "10")
//						.set("spark.default.parallelism", "100")
//						.set("spark.storage.memoryFraction", "0.5")  
//						.set("spark.shuffle.consolidateFiles", "true")
//						.set("spark.shuffle.file.buffer", "64")  
//						.set("spark.shuffle.memoryFraction", "0.3")    
//						.set("spark.reducer.maxSizeInFlight", "24")  
//						.set("spark.shuffle.io.maxRetries", "60")  
//						.set("spark.shuffle.io.retryWait", "60")   
//						.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//						.registerKryoClasses(new Class[]{
//								SpeedSortKey.class})
                ;

        /**
         * 设置spark运行时的master  根据配置文件来决定的
         */
        SparkUtils.setMaster(conf);


        JavaSparkContext sc = new JavaSparkContext(conf);


        SQLContext sqlContext = SparkUtils.getSQLContext(sc);

        /**
         * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的临时表就可以
         * 本地模拟数据注册成一张临时表
         * monitor_flow_action
         */
        SparkUtils.mockData(sc, sqlContext);


        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR);


        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findTaskById(taskId);

        if (task == null) {
            return;
        }


        JSONObject taskParamsJsonObject = JSONObject.parseObject(task.getTaskParams());

        JavaRDD<Row> cameraRDD = SparkUtils.getCameraRDDByDateRange(sqlContext, taskParamsJsonObject);

        cameraRDD = cameraRDD.cache();

        Accumulator<String> monitorAndCameraStateAccumulator = sc.accumulator("", new MonitorAndCameraStateAccumulator());

        /**
         * 将cameraRDD变成kv格式的rdd
         */
        JavaPairRDD<String, Row> monitor2DetailRDD = getMonitor2DetailRDD(cameraRDD);

        monitor2DetailRDD = monitor2DetailRDD.cache();

        /**
         * 以monitor_Id为单位进行聚合，如何以monitor_id作为单位的？
         * groupByKey
         * monitor_id,infos(area_id,monitor_id,camera_count,camera_ids,car_count)
         */
        JavaPairRDD<String, String> aggregateMonitorId2DetailRDD = aggreagteByMonitor(monitor2DetailRDD);
        JavaPairRDD<Integer, String> carCount2MonitorRDD = checkMonitorState(sc, sqlContext, aggregateMonitorId2DetailRDD, taskId, taskParamsJsonObject, monitorAndCameraStateAccumulator);

        //获取卡口流量的前N名，并且持久化到数据库中
        //topNMonitor2CarFlow  key:monitor_id  v:monitor_id
        JavaPairRDD<String, String> topNMonitor2CarFlow = getTopNMonitorCarFlow(sc, taskId, taskParamsJsonObject, carCount2MonitorRDD);

        saveMonitorState(taskId, monitorAndCameraStateAccumulator);

        //获取topN 卡口的具体信息
        getTopNDetails(taskId, topNMonitor2CarFlow, monitor2DetailRDD);

        List<String> top50MonitorIds = speedTopNMonitor(monitor2DetailRDD);
        for (String monitorId : top50MonitorIds) {
            System.out.println("monitorId:" + monitorId);
        }

        /**
         * 高速通过的top50的monitorId
         * 每一个monitorId下面再找出来通过速度最快的top10
         * 一共会有50 *１０　＝　５００　
         *1、根据top50这个列表，过滤
         *2、根据过滤出来的结果  分组 groupByKey（monitorId）
         *3、组内排序（
         *	注意：尽量避免使用集合类。为什么？
         *		有可能这一个monitorId下面所对应的数据量非常的大，OOM
         *），然后取出来topN
         *
         *monitor2DetailRDD
         *	key:monitor_id
         *	value:row
         */
        getMonitorDetails(sc, taskId, top50MonitorIds, monitor2DetailRDD);

        sc.close();
    }


    private static Map<String, String> getAreaInfosFromDB() {
        IAreaDao areaDao = DAOFactory.getAreaDao();

        List<Area> findAreaInfo = areaDao.findAreaInfo();

        Map<String, String> areaMap = new HashMap<>();
        for (Area area : findAreaInfo) {
            areaMap.put(area.getAreaId(), area.getAreaName());
        }
        return areaMap;
    }


    /**
     * 50个卡扣   50个卡扣下  速度top10的车辆         500条记录？       分组取top10   groupByKey按照key进行了分组，然后对组内的数据进行倒叙排序，然后取top10
     *
     * @param sc
     * @param taskId
     * @param top10MonitorIds
     * @param monitor2DetailRDD
     */
    private static void getMonitorDetails(JavaSparkContext sc, final long taskId, List<String> top10MonitorIds, JavaPairRDD<String, Row> monitor2DetailRDD) {
        /**
         * 使用join能完成吗？
         * 		如果使用join     top10MonitorIds     List<Tuple2<String,String>> list = new ArrrayList<>();
         *
         * 只有区区50条记录，没必要产生shuffle     50亿呢？   filter做不到   map做不到  broadCast也做不到     唯一办法就是使用join  产生shuffle
         */
        final Broadcast<List<String>> top10MonitorIdsBroadcast = sc.broadcast(top10MonitorIds);

        monitor2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                String monitorIds = tuple._1;
                List<String> list = top10MonitorIdsBroadcast.value();
                return list.contains(monitorIds);
            }
            /**
             * 如果我想在foreach中的计算结果在Driver段看到
             * 1、累加器（Driver段定义，Executor段操作，Driver段读取）
             * 2、map（业务逻辑）   新的rdd   rdd.collect
             */
        }).groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Row>>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                Iterator<Row> rowsIterator = tuple._2.iterator();

                /**
                 * 这里面存放的是row对象（车辆的具体信息  ）
                 * 凭借什么来存放？
                 * 	Speed
                 *
                 * 这里面为什么不使用List  Collections.sort(list)
                 *
                 *  如果我们算的是一个月的数据呢？     那么这一个卡扣下的车流量会非常多大，会有OOM
                 */
                Row[] top10Cars = new Row[10];
                while (rowsIterator.hasNext()) {
                    Row row = rowsIterator.next();

                    long speed = Long.valueOf(row.getString(5));

                    for (int i = 0; i < top10Cars.length; i++) {
                        if (top10Cars[i] == null) {
                            top10Cars[i] = row;
                            break;
                        } else {
                            long _speed = Long.valueOf(top10Cars[i].getString(5));
                            if (speed > _speed) {
                                for (int j = 9; j > i; j--) {
                                    top10Cars[j] = top10Cars[j - 1];
                                }
                                top10Cars[i] = row;
                                break;
                            }
                        }
                    }
                }
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                List<TopNMonitorDetailInfo> topNMonitorDetailInfos = new ArrayList<>();
                for (Row row : top10Cars) {
                    topNMonitorDetailInfos.add(new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6)));
                }
                monitorDAO.insertBatchTop10Details(topNMonitorDetailInfos);
            }
        });
    }


    /**
     * 1、每一辆车都有speed    按照速度划分是否是高速 中速 普通 低速
     * 2、每一辆车的车速都在一个车速段     对每一个卡扣进行聚合   拿到高速通过 中速通过  普通  低速通过的车辆各是多少辆
     * 3、四次排序   先按照高速通过车辆数   中速通过车辆数   普通通过车辆数   低速通过车辆数
     *
     * @param cameraRDD
     * @return
     */

    private static List<String> speedTopNMonitor(JavaPairRDD<String, Row> monitorId2DetailRDD) {
        JavaPairRDD<String, Iterable<Row>> groupByMonitorId = monitorId2DetailRDD.groupByKey();


        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> speedSortKey2MonitorId = groupByMonitorId.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, SpeedSortKey, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<SpeedSortKey, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String monitorId = tuple._1;
                Iterator<Row> speedIterator = tuple._2.iterator();

                long lowSpeed = 0;
                long normalSpeed = 0;
                long mediumSpeed = 0;
                long highSpeed = 0;

                while (speedIterator.hasNext()) {
                    int speed = StringUtils.convertStringtoInt(speedIterator.next().getString(5));
                    if (speed >= 0 && speed < 60) {
                        lowSpeed++;
                    } else if (speed >= 60 && speed < 90) {
                        normalSpeed++;
                    } else if (speed >= 90 && speed < 120) {
                        mediumSpeed++;
                    } else if (speed >= 120) {
                        highSpeed++;
                    }
                }
                SpeedSortKey speedSortKey = new SpeedSortKey(lowSpeed, normalSpeed, mediumSpeed, highSpeed);
                return new Tuple2<SpeedSortKey, String>(speedSortKey, monitorId);
            }
        });
        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> sortBySpeedCount = speedSortKey2MonitorId.sortByKey(false);
        List<Tuple2<SpeedSortKey, String>> take = sortBySpeedCount.take(50);

        /**
         * 最畅通的top50
         * 这个list里面有50条记录
         */
        List<String> monitorIds = new ArrayList<>();
        for (Tuple2<SpeedSortKey, String> tuple : take) {
            monitorIds.add(tuple._2);
        }

        return monitorIds;
    }


    /**
     * 按照monitor_进行聚合，cameraId camer_count
     *
     * @param monitorId2Detail
     * @return
     */
    private static JavaPairRDD<String, String> aggreagteByMonitor(JavaPairRDD<String, Row> monitorId2Detail) {
        /**
         * <monitor_id,List<Row> 集合里面的记录代表的是camera的信息。>
         */
        JavaPairRDD<String, Iterable<Row>> monitorId2RowRDD = monitorId2Detail.groupByKey();


        /**
         * 一个monitor_id对应一条记录
         * 为什么使用mapToPair来遍历数据，因为我们要操作的返回值是每一个monitorid 所对应的详细信息
         */
        JavaPairRDD<String, String> monitorId2CameraCountRDD = monitorId2RowRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String monitorId = tuple._1;
                Iterator<Row> rowIterator = tuple._2.iterator();

                List<String> list = new ArrayList<>();

                StringBuilder tmpInfos = new StringBuilder();

                int count = 0;
                String areaId = "";
                /**
                 * 这个while循环  代表的是这个卡扣一共经过了多少辆车   一辆车的信息就是一个row
                 */
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    areaId = row.getString(7);
                    String cameraId = row.getString(2);
                    if (!list.contains(cameraId)) {
                        list.add(cameraId);
                    }
                    if (!tmpInfos.toString().contains(cameraId)) {
                        tmpInfos.append("," + row.getString(2));
                    }
                    count++;
                }
                /**
                 * camera的个数
                 */
                int cameraCount = list.size();

                String infos = Constants.FIELD_MONITOR_ID + "=" + monitorId + "|" +
                        Constants.FIELD_AREA_ID + "=" + areaId + "|" +
                        Constants.FIELD_CAMERA_IDS + "=" + tmpInfos.toString().substring(1) + "|" +
                        Constants.FIELD_CAMERA_COUNT + "=" + cameraCount + "|"
                        + Constants.FIELD_CAR_COUNT + "=" + count;
                return new Tuple2<String, String>(monitorId, infos);
            }
        });
        //<monitor_id,camera_infos(ids,cameracount,carCount)>
        return monitorId2CameraCountRDD;
    }


    private static void saveMonitorState(Long taskId, Accumulator<String> monitorAndCameraStateAccumulator) {
        /**
         * 累加器中值能在Executor段读取吗？
         * 		不能
         * 这里的读取时在Driver中进行的
         */
        String accumulatorVal = monitorAndCameraStateAccumulator.value();
        String normalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_MONITOR_COUNT);
        String normalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_CAMERA_COUNT);
        String abnormalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_COUNT);
        String abnormalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_CAMERA_COUNT);
        String abnormalMonitorCameraInfos = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS);

        /**
         * 这里面只有一条记录
         */
        MonitorState monitorState = new MonitorState(taskId, normalMonitorCount, normalCameraCount, abnormalMonitorCount, abnormalCameraCount, abnormalMonitorCameraInfos);

        IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
        monitorDAO.insertMonitorState(monitorState);
    }

    private static JavaPairRDD<String, Row> getMonitor2DetailRDD(JavaRDD<Row> cameraRDD) {
        JavaPairRDD<String, Row> monitorId2Detail = cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(1), row);
            }
        });
        return monitorId2Detail;
    }

    private static void getTopNDetails(final long taskId, JavaPairRDD<String, String> topNMonitor2CarFlow, JavaPairRDD<String, Row> monitor2DetailRDD) {
        /**
         * 获取车流量排名前N的卡口的详细信息   可以看一下是在什么时间段内卡口流量暴增的。
         */


        topNMonitor2CarFlow.join(monitor2DetailRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> t) throws Exception {
                return new Tuple2<String, Row>(t._1, t._2._2);
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Row>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, Row>> t) throws Exception {

                List<TopNMonitorDetailInfo> monitorDetailInfos = new ArrayList<>();
                while (t.hasNext()) {
                    Tuple2<String, Row> tuple = t.next();
                    Row row = tuple._2;
                    TopNMonitorDetailInfo m = new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
                    monitorDetailInfos.add(m);
                }

                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                monitorDAO.insertBatchMonitorDetails(monitorDetailInfos);
            }
        });
        ;


    }

    /**
     * 获取卡口流量的前三名，并且持久化到数据库中
     *
     * @param taskId
     * @param taskParamsJsonObject
     * @param carCount2MonitorId
     */
    private static JavaPairRDD<String, String> getTopNMonitorCarFlow(JavaSparkContext sc, long taskId, JSONObject taskParamsJsonObject, JavaPairRDD<Integer, String> carCount2MonitorId) {
        /**
         * 获取车流量排名前三的卡口信息
         * 			有什么作用？ 当某一个卡口的流量这几天突然暴增和往常的流量不相符，交管部门应该找一下原因，是什么问题导致的，应该到现场去疏导车辆。
         */
        int topNumFromParams = Integer.parseInt(ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_TOP_NUM));

        /**
         * carCount2MonitorId <carCount,monitor_id>
         */
        List<Tuple2<Integer, String>> topNCarCount = carCount2MonitorId.sortByKey(false).take(topNumFromParams);


        List<TopNMonitor2CarCount> topNMonitor2CarCounts = new ArrayList<>();
        for (Tuple2<Integer, String> tuple : topNCarCount) {
            TopNMonitor2CarCount topNMonitor2CarCount = new TopNMonitor2CarCount(taskId, tuple._2, tuple._1);
            topNMonitor2CarCounts.add(topNMonitor2CarCount);
        }

        IMonitorDAO ITopNMonitor2CarCountDAO = DAOFactory.getMonitorDAO();
        ITopNMonitor2CarCountDAO.insertBatchTopN(topNMonitor2CarCounts);


        List<Tuple2<String, String>> monitorId2CarCounts = new ArrayList<>();
        for (Tuple2<Integer, String> t : topNCarCount) {
            monitorId2CarCounts.add(new Tuple2<String, String>(t._2, t._2));
        }
        JavaPairRDD<String, String> monitorId2CarCountRDD = sc.parallelizePairs(monitorId2CarCounts);
        return monitorId2CarCountRDD;
    }


    /**
     * 检测卡口状态
     *
     * @param sc
     * @param cameraRDD
     */
    private static JavaPairRDD<Integer, String> checkMonitorState(JavaSparkContext sc, SQLContext sqlContext, JavaPairRDD<String, String> monitorId2CameraCountRDD, final long taskId, JSONObject taskParamsJsonObject, final Accumulator<String> monitorAndCameraStateAccumulator) {
        /**
         * 从monitor_camera_info表中查询出来每一个卡口对应的camera的数量
         */
        String sqlText = ""
                + "SELECT * "
                + "FROM monitor_camera_info";
        DataFrame standardDF = sqlContext.sql(sqlText);
        JavaRDD<Row> standardRDD = standardDF.javaRDD();
        JavaPairRDD<String, String> monitorId2CameraId = standardRDD.mapToPair(new PairFunction<Row, String, String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(0), row.getString(1));
            }
        });

        JavaPairRDD<String, String> standardDonitor2CameraInfos = monitorId2CameraId.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String monitorId = tuple._1;
                Iterator<String> cameraIterator = tuple._2.iterator();
                int count = 0;
                StringBuilder cameraIds = new StringBuilder();
                while (cameraIterator.hasNext()) {
                    cameraIds.append("," + cameraIterator.next());
                    count++;
                }
                String cameraInfos = Constants.FIELD_CAMERA_IDS + "=" + cameraIds.toString().substring(1) + "|" +
                        Constants.FIELD_CAMERA_COUNT + "=" + count;
                return new Tuple2<String, String>(monitorId, cameraInfos);
            }
        });

        /**
         * rdd1:1	198
         * rdd2:2	987
         * 		1	765
         *
         * 1 	198	null
         * 1	198	765
         */
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinResultRDD = standardDonitor2CameraInfos.leftOuterJoin(monitorId2CameraCountRDD);

        JavaPairRDD<Integer, String> carCount2MonitorId = joinResultRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<String, Optional<String>>>>, Integer, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<Tuple2<Integer, String>> call(Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> iterator) throws Exception {
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    //储藏返回值
                    Tuple2<String, Tuple2<String, Optional<String>>> tuple = iterator.next();
                    String monitorId = tuple._1;
                    String standardCameraInfos = tuple._2._1;
                    Optional<String> factCameraInfosOptional = tuple._2._2;
                    String factCameraInfos = "";

                    if (factCameraInfosOptional.isPresent()) {
                        factCameraInfos = factCameraInfosOptional.get();
                    } else {
                        continue;
                    }

                    int factCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                    int standardCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                    if (factCameraCount == standardCameraCount) {
                        /*
                         * 	1、正常卡口数量
                         * 	2、异常卡口数量
                         * 	3、正常通道（此通道的摄像头运行正常）数
                         * 	4、异常卡口数量中哪些摄像头异常，需要保存摄像头的编号
                         */
                        monitorAndCameraStateAccumulator.add(Constants.FIELD_NORMAL_MONITOR_COUNT + "=1|" + Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + factCameraCount);
                    } else {
                        String factCameraIds = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                        String standardCameraIds = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                        List<String> factCameraIdList = Arrays.asList(factCameraIds.split(","));
                        List<String> standardCameraIdList = Arrays.asList(standardCameraIds.split(","));
                        StringBuilder abnormalCameraInfos = new StringBuilder();
//						System.out.println("factCameraIdList:"+factCameraIdList);
//						System.out.println("standardCameraIdList:"+standardCameraIdList);
                        int abnormalCmeraCount = 0;
                        int normalCameraCount = 0;
                        for (String str : standardCameraIdList) {
                            if (!factCameraIdList.contains(str)) {
                                abnormalCmeraCount++;
                                abnormalCameraInfos.append("," + str);
                            }
                        }
                        normalCameraCount = standardCameraIdList.size() - abnormalCmeraCount;
                        monitorAndCameraStateAccumulator.add(Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + normalCameraCount + "|" +
                                Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|" +
                                Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnormalCmeraCount + "|" +
                                Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + abnormalCameraInfos.toString().substring(1));
                    }

                    int carCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAR_COUNT));
                    list.add(new Tuple2<Integer, String>(carCount, monitorId));
                }
                return list;
            }
        });
        return carCount2MonitorId;
    }
}

