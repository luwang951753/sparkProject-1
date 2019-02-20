package com.ish.sparkproject.dao.impl;


import com.ish.sparkproject.dao.IAdUserClickCountDAO;
import com.ish.sparkproject.domain.AdUserClickCount;
import com.ish.sparkproject.jdbc.JDBCHelper;
import com.ish.sparkproject.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {

	@Override
	public void updateBatch(List<AdUserClickCount> adUserClickCounts) {

		List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<AdUserClickCount>();
		List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<AdUserClickCount>();

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();

		String selectSQL =
				"SELECT count(*) " +
				"FROM ad_user_click_count " +
				"WHERE date = ? " +
				"AND userid = ? " +
				"AND adid = ?;";

		Object[] selectParams = null;
		for (AdUserClickCount adUserClickCount : adUserClickCounts) {

			AdUserClickCountQueryResult adUserClickCountQueryResult =
					new AdUserClickCountQueryResult();

			selectParams = new Object[]{
					adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid(),
					adUserClickCount.getClickCount()
			};

			jdbcHelper.executeQuery(selectSQL, selectParams, new JDBCHelper.QureyCallback() {
				@Override
				public void process(ResultSet rs) throws Exception {
					if (rs.next()){
						int count = rs.getInt(1);
						adUserClickCountQueryResult.setCount(count);
					}
				}
			});

			int count = adUserClickCountQueryResult.getCount();
			if (count > 0) {
				updateAdUserClickCounts.add(adUserClickCount);
			}else {
				insertAdUserClickCounts.add(adUserClickCount);
			}
		}

		// count <= 0, 执行批量数据插入
		String insertSQL =
				"INSERT INTO " +
					"ad_user_click_count " +
				"VALUES" +
					"(?,?,?,?)";

		List<Object[]> insertParamList = new ArrayList<Object[]>();
		Object[] insertParams = new Object[4];
		for (AdUserClickCount adUserClickCount: insertAdUserClickCounts) {
			insertParams[0] = adUserClickCount.getDate();
			insertParams[1] = adUserClickCount.getUserid();
			insertParams[2] = adUserClickCount.getAdid();
			insertParams[3] = adUserClickCount.getClickCount();
			insertParamList.add(insertParams);
		}
		jdbcHelper.executeBatch(insertSQL, insertParamList);

		// count > 0, 执行批量数据更新
		String updateSQL =
				"UPDATE ad_user_click_count " +
				"SET click_count = click_count + ? " +
				"WHERE date = ? " +
				"AND userid = ? " +
				"AND adid = ?;";
		List<Object[]> updateParamList = new ArrayList<Object[]>();
		Object[] updateParams = new Object[4];
		for (AdUserClickCount adUserClickCount: updateAdUserClickCounts) {
			updateParams[0] = adUserClickCount.getClickCount();
			updateParams[1] = adUserClickCount.getDate();
			updateParams[2] = adUserClickCount.getUserid();
			updateParams[3] = adUserClickCount.getAdid();

			updateParamList.add(updateParams);
		}
		jdbcHelper.executeBatch(updateSQL, updateParamList);
	}

	/**
	 * 根据多个key查询对应条件的用户广告点击量clickCount;
	 * @param date
	 * @param userid
	 * @param adid
	 * @return
	 */
	@Override
	public int findClickCountByMultiKey(String date, long userid, long adid) {
		String sql =
				"SELECT " +
					"click_count " +
				"FROM " +
					"ad_user_click_count " +
				"WHERE " +
					"date = ? " +
				"AND " +
					"userid = ? " +
				"AND " +
					"adid = ?";
		Object[] params = new Object[]{date, userid, adid};
		AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
		JDBCHelper.getInstance().executeQuery(sql, params, new JDBCHelper.QureyCallback() {
			@Override
			public void process(ResultSet rs) throws Exception {
				if (rs.next()){
					int clickCount = rs.getInt(1);
					queryResult.setClickCount(clickCount);
				}
			}
		});
		int clickCount = queryResult.getClickCount();

		return clickCount;
	}
}
