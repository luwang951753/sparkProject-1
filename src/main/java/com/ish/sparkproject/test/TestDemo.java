package com.ish.sparkproject.test;

import com.ish.sparkproject.utils.DateUtils;
import com.ish.sparkproject.utils.StringUtils;

import java.util.*;

public class TestDemo {
	public static void main(String[] args) {

//		Map<String, Integer> hourCountMap = new HashMap();
//		hourCountMap.put("2018-02-18_14", 11);
//		hourCountMap.put("2018-02-18_15", 56);
//		hourCountMap.put("2018-02-18_16", 34);
//		hourCountMap.put("2018-02-19_15", 15);
//		hourCountMap.put("2018-02-19_17", 21);
//
//		Map<String,Map<String, Integer>> dayHourCountMap =
//				new HashMap<String,Map<String, Integer>>();
//		for (Map.Entry<String, Integer> countEntry : hourCountMap.entrySet()) {
//			String date = countEntry.getKey().split("_")[0];
//			String hour = countEntry.getKey().split("_")[1];
//			Integer count = countEntry.getValue();
//			Map<String, Integer> hourCount = dayHourCountMap.get(date);
//
//			if (hourCount == null){
//				hourCount = new HashMap<String, Integer>();
//				dayHourCountMap.put(date, hourCount);
//			}
//			hourCount.put(hour, count);
//		}
//
//		System.out.println(dayHourCountMap);


//		// 构建Spark的上下文
//		SparkSession sparkSession = SparkSession.builder()
//				.appName(Constants.SPARK_APP_NAME)
//				.master("local[2]")
//				.getOrCreate();
//
//		List<String> list = Arrays.asList("1s_3s","4s_6s","7s_9s","30_60","60");
//
//		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
//		JavaRDD<String> listRDD = jsc.parallelize(list);
//		SessionAggrStatAccumulator acc = new SessionAggrStatAccumulator();
//		sparkSession.sparkContext().register(acc, "acc");
//		listRDD.foreach(e-> acc.add(e));
//		listRDD.collect();
//		System.out.println(acc.value());
//		sparkSession.stop();

//		System.out.println(DateUtils.parseTime("2019-01-06 8:45:11"));
//		System.out.println(DateUtils.parseTime("2019-01-06 0:23:08"));
//		System.out.println(DateUtils.getTodayDate());

//		Random random = new Random();
//		String date = DateUtils.getTodayDate();
//		System.out.println(date);
//		String baseActionTime =
//				date + " " + StringUtils.fulfuill(String.valueOf(random.nextInt(24)));
//
//		System.out.println("2019-01-06 12:57:47".length());

		Map<String, Integer> map = new HashMap<>();
		map.put("a",1);
		map.put("b",2);
		map.put("c",3);
		map.put("d",4);
		for (Map.Entry entry : map.entrySet()) {
			System.out.println(entry.getKey()+ "=" + entry.getValue());
		}

		System.out.println("=============分割线=============");

		map.put("a",10);
		for (Map.Entry entry : map.entrySet()) {
			System.out.println(entry.getKey()+ "=" + entry.getValue());
		}
	}
}
