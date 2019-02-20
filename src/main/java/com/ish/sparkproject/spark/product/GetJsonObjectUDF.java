package com.ish.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

public class GetJsonObjectUDF implements UDF2<String, String, String> {
	@Override
	public String call(String json, String field) throws Exception {
		JSONObject jsonObject = null;
		try {
			jsonObject = JSONObject.parseObject(json);
			return jsonObject.getString(field);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
