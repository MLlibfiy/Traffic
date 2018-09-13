package com.shujia.spark.areaRoadFlow;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import com.shujia.spark.util.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * 组内拼接去重函数（group_concat_distinct()）
 * 
 * 技术点4：自定义UDAF聚合函数
 * 
 * @author Administrator
 *
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

	private static final long serialVersionUID = -2510776241322950505L;
	
	// 指定输入数据的字段与类型
	private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("carInfo", DataTypes.StringType, true)));  
	// 指定缓冲数据的字段与类型
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("bufferInfo", DataTypes.StringType, true)));  
	// 指定返回类型
	private DataType dataType = DataTypes.StringType;
	// 指定是否是确定性的
	private boolean deterministic = true;
	
	@Override
	public StructType inputSchema() {
		return inputSchema;
	}
	
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}
	
	/**
	 * 初始化
	 * 可以认为是，你自己在内部指定一个初始的值
	 */
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");  
	}
	
	/**
	 * 更新
	 * 可以认为是，一个一个地将组内的字段值传递进来
	 * 实现拼接的逻辑
	 */
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		// 缓冲中的已经拼接过的monitor信息小字符串
		String bufferMonitorInfo = buffer.getString(0);
		// 刚刚传递进来的某个车辆信息
		String inputMonitorInfo = input.getString(0);
		
		String[] split = inputMonitorInfo.split("\\|");
		String monitorId = "";
		int addNum = 1;
		for (String string : split) {
			if(string.indexOf("=") != -1){
				monitorId = string.split("=")[0];
				addNum = Integer.parseInt(string.split("=")[1]);
			}else{
				monitorId = string;
			}
			String oldVS = StringUtils.getFieldFromConcatString(bufferMonitorInfo, "\\|", monitorId);
			if(oldVS == null) {
				bufferMonitorInfo += "|"+monitorId+"="+addNum;
			}else{
				bufferMonitorInfo = StringUtils.setFieldInConcatString(bufferMonitorInfo, "\\|", monitorId, Integer.parseInt(oldVS)+addNum+"");
			}
			buffer.update(0, bufferMonitorInfo);  
		}
	}

	/**
	 * [02,5,135,0003=21|0008=15|0005=14|0000=13|0001=10|0004=19|0002=12|0007=18|0006=13]
	 * [06,18,165,0004=24|0008=14|0001=28|0000=22|0007=11|0005=17|0003=17|0002=10|0006=22]
	 * [02,6,165,0008=23|0006=21|0004=20|0005=15|0007=24|0002=14|0003=21|0001=12|0000=15]
	 * [06,19,142,0006=14|0004=20|0001=19|0002=14|0003=15|0005=16|0007=12|0000=18|0008=14]
	 * [02,7,153,0007=25|0003=21|0006=21|0008=11|0002=11|0005=15|0004=14|0000=23|0001=12]
	 * [02,8,144,0003=12|0006=17|0001=21|0004=18|0002=15|0000=14|0007=17|0008=14|0005=16]
	 * [02,9,162,0008=20|0005=20|0004=16|0006=10|0002=10|0003=26|0001=21|0000=20|0007=19]
	 * [03,50,133,0008=15|0006=15|0000=13|0001=21|0004=17|0002=13|0003=12|0007=15|0005=12]
	 * [01,10,141,0000=18|0005=22|0003=16|0007=9|0008=15|0002=19|0004=10|0001=14|0006=18]
	 * [01,11,153,0000=20|0006=19|0005=10|0008=14|0002=22|0007=11|0001=21|0003=15|0004=21]
	 *
	 */
	
	/**
	 * 合并
	 * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的
	 * 但是可能一个分组内的数据，会分布在多个节点上处理
	 * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
	 */
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		//缓存中的monitor信息这个大字符串
		String bufferMonitorInfo1 = buffer1.getString(0);
		//传进来          
		String bufferMonitorInfo2 = buffer2.getString(0);

        // 等于是把buffer2里面的数据都拆开来更新
		for(String monitorInfo : bufferMonitorInfo2.split("\\|")) {
			/**
			 * monitor_id1 100
			 * monitor_id2 88
			 */
			Map<String, String> map = StringUtils.getKeyValuesFromConcatString(monitorInfo, "\\|");
			for (Entry<String, String> entry : map.entrySet()) {
				String monitor = entry.getKey();
				int carCount = Integer.parseInt(entry.getValue());
				String oldVS = StringUtils.getFieldFromConcatString(bufferMonitorInfo1, "\\|", monitor);
				if(oldVS == null) {
					if("".equals(bufferMonitorInfo1)) {
						bufferMonitorInfo1 += monitor + "=" + carCount;
					} else {
						bufferMonitorInfo1 += "|" + monitor + "=" + carCount;
					}
	 			}else{
	 				int oldVal = Integer.valueOf(StringUtils.getFieldFromConcatString(bufferMonitorInfo1, "\\|", monitor));
	 				oldVal += carCount;
	 				bufferMonitorInfo1 = StringUtils.setFieldInConcatString(bufferMonitorInfo1, "\\|", monitor, oldVal+"");
	 			}
				buffer1.update(0, bufferMonitorInfo1);  
			}
		}
	}
	
	@Override
	public DataType dataType() {
		return dataType;
	}
	
	/**
	 * evaluate方法返回数据的类型要和dateType的类型一致，不一致就会报错
	 */
	@Override
	public Object evaluate(Row row) {  
		return row.getString(0);  
	}

}
