package com.shujia.spark.dao.impl;

import com.shujia.spark.constant.Constants;
import com.shujia.spark.dao.IWithTheCarDAO;
import com.shujia.spark.jdbc.JDBCHelper;
import com.shujia.spark.util.DateUtils;

public class WithTheCarDAOImpl implements IWithTheCarDAO {

	@Override
	public void updateTestData(String cars) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		String sql = "UPDATE task set task_param = ? WHERE task_id = 3";
		Object[] params = new Object[]{"{\"startDate\":[\""+DateUtils.getTodayDate()+"\"],\"endDate\":[\""+DateUtils.getTodayDate()+"\"],\""+Constants.FIELD_CARS+"\":[\""+cars+"\"]}"};
		jdbcHelper.executeUpdate(sql, params);
	}

}
