package com.ish.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF1;

public class RemoveRandomPrefixUDF implements UDF1<String, String> {
	@Override
	public String call(String key) throws Exception {
		String[] splited = key.split("_");
		return splited[1];
	}
}
