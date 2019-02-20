package com.ish.sparkproject.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * 组内拼接去重函数
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
	public GroupConcatDistinctUDAF() {
		super();
	}

	// 指定输入数据的字段和类型
	private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));
	// 指定缓冲数据的字段和类型
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
	// 返回值数据结构
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
	public DataType dataType() {
		return dataType;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}

	// 初始化缓冲区
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}

	// 给聚合函数传入一条新数据进行处理
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		String bufferCityInfo = buffer.getString(0);
		String cityInfo = input.getString(0);
		// 实现去重的逻辑
		if (!bufferCityInfo.contains(cityInfo)){
			if ("".equals(bufferCityInfo)){
				bufferCityInfo += cityInfo;
			}else {
				bufferCityInfo += "," + cityInfo;
			}
		}
		buffer.update(0, bufferCityInfo);
	}

	// 合并聚合函数缓冲区
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String bufferCityInfo1 = buffer1.getString(0);
		String bufferCityInfo2 = buffer2.getString(0);

		for (String cityInfo: bufferCityInfo2.split(",")) {
			if (!bufferCityInfo1.contains(cityInfo)){
				if ("".equals(bufferCityInfo1)){
					bufferCityInfo1 += cityInfo;
				}else {
					bufferCityInfo1 += "," + cityInfo;
				}
			}
		}
		buffer1.update(0, bufferCityInfo1);
	}

	@Override
	public Object evaluate(Row buffer) {
		return buffer.getString(0);
	}

}
