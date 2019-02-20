package com.ish.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * 给键加随机前缀
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {
	@Override
	public String call(String key, Integer num) throws Exception {
		Random random = new Random();
		int randNum = random.nextInt(10);
		return randNum + "_" + key;
	}
}
