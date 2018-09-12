package com.shujia.spark.dao;

import java.util.List;

import com.shujia.spark.domain.CarTrack;

public interface ICarTrackDAO {
	
	/**
	 * 批量插入车辆轨迹信息
	 * @param carTracks
	 */
	void insertBatchCarTrack(List<CarTrack> carTracks);
}
