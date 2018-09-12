package com.shujia.spark.dao.factory;

import com.shujia.spark.dao.IAreaDao;
import com.shujia.spark.dao.ICarTrackDAO;
import com.shujia.spark.dao.IMonitorDAO;
import com.shujia.spark.dao.IRandomExtractDAO;
import com.shujia.spark.dao.ITaskDAO;
import com.shujia.spark.dao.IWithTheCarDAO;
import com.shujia.spark.dao.impl.AreaDaoImpl;
import com.shujia.spark.dao.impl.CarTrackDAOImpl;
import com.shujia.spark.dao.impl.MonitorDAOImpl;
import com.shujia.spark.dao.impl.RandomExtractDAOImpl;
import com.shujia.spark.dao.impl.TaskDAOImpl;
import com.shujia.spark.dao.impl.WithTheCarDAOImpl;

/**
 * DAO工厂类
 * @author root
 *
 */
public class DAOFactory {
	
	
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
	
	public static IMonitorDAO getMonitorDAO(){
		return new MonitorDAOImpl();
	}
	
	public static IRandomExtractDAO getRandomExtractDAO(){
		return new RandomExtractDAOImpl();
	}
	
	public static ICarTrackDAO getCarTrackDAO(){
		return new CarTrackDAOImpl();
	}
	
	public static IWithTheCarDAO getWithTheCarDAO(){
		return new WithTheCarDAOImpl();
	}

	public static IAreaDao getAreaDao() {
		return  new AreaDaoImpl();
		
	}
}
