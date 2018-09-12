package com.shujia.spark.dao;

import java.util.List;

import com.shujia.spark.domain.RandomExtractCar;
import com.shujia.spark.domain.RandomExtractMonitorDetail;

/**
 * 随机抽取car信息管理DAO类
 * @author root
 *
 */
public interface IRandomExtractDAO {
	void insertBatchRandomExtractCar(List<RandomExtractCar> carRandomExtracts);
	
	void insertBatchRandomExtractDetails(List<RandomExtractMonitorDetail> r);
	
}
