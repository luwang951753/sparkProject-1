package com.ish.sparkproject.dao;

import com.ish.sparkproject.domain.AdUserClickCount;

import java.util.List;

/**
 * 用户广告点击量DAO接口
 */
public interface IAdUserClickCountDAO {

	/*
	* 批量更新用户广告点击量clickCount到MySQL
	* */
	void updateBatch(List<AdUserClickCount> adUserClickCounts);

	/*
	* 根据复合key对点击量clickCount进行查询
	* */
	int findClickCountByMultiKey(String date, long userid, long adid);
}
