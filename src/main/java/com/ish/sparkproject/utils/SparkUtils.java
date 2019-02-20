package com.ish.sparkproject.utils;

import com.alibaba.fastjson.JSONObject;
import com.ish.sparkproject.constant.Constants;
import com.ish.sparkproject.test.MockData;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkUtils {
	/**
	 * 生成模拟数据的方法
	 * @param sparkSession
	 */
	public static void mockData(SparkSession sparkSession){
		MockData.mock(sparkSession);
	}

	/**
	 * 按照日期范围,筛选出用户访问行为数据
	 * @param sparkSession
	 * @param taskParams
	 * @return
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(
			SparkSession sparkSession, JSONObject taskParams){

		String startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE);
		String sql =
				"select * "
						+ "from user_visit_action "
						+ "where date >='" + startDate + "'"
						+ "and date <='" + endDate + "'";
		Dataset<Row> actionDF = sparkSession.sql(sql);
		actionDF.show();
		return actionDF.javaRDD();
	}

	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD){
		JavaPairRDD<String, Row>sessionid2Row = actionRDD.mapToPair(row -> {
			String sessionid = row.getString(2);
			return new Tuple2<>(sessionid, row);
		});
		return sessionid2Row;
	}

}
