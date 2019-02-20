package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.IAdStatDAO;
import com.ish.sparkproject.domain.AdStat;
import com.ish.sparkproject.jdbc.JDBCHelper;
import com.ish.sparkproject.model.AdStatQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdStatDAOImpl implements IAdStatDAO {
	@Override
	public void updateBatch(List<AdStat> adStats) {
		List<AdStat> insertAdStats = new ArrayList<>();
		List<AdStat> updateAdStats = new ArrayList<>();

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		String querySQL =
				"SELECT count(*) " +
				"FROM ad_stat " +
				"WHERE date = ? " +
				"AND province = ? " +
				"AND city = ? " +
				"AND ad_id = ?";

		Object[] selectParams = null;
		for (AdStat adStat : adStats) {
			AdStatQueryResult adStatQueryResult = new AdStatQueryResult();
			selectParams = new Object[]{
					adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getAdid(),
					adStat.getClickCount()
			};
			jdbcHelper.executeQuery(querySQL, selectParams, new JDBCHelper.QureyCallback() {
				@Override
				public void process(ResultSet rs) throws Exception {
					if (rs.next()){
						adStatQueryResult.setCount(rs.getInt(1));
					}
				}
			});
			int count = adStatQueryResult.getCount();
			if (count > 0){
				updateAdStats.add(adStat);
			}else {
				insertAdStats.add(adStat);
			}
		}

		// count > 0 ,执行批量更新
		String updateSQL =
				"UPDATE ad_stat " +
				"SET click_count = ? " +
				"WHERE date = ? " +
				"AND province = ? " +
				"AND city = ? " +
				"AND adid = ?;";
		List<Object[]> updateParamList = new ArrayList<>();
		for (AdStat adStat : updateAdStats){
			Object[] params = new Object[]{
					adStat.getClickCount(),
					adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getAdid()
			};
			updateParamList.add(params);
		}
		jdbcHelper.executeBatch(updateSQL, updateParamList);

		// count <= 0 ,执行批量插入
		String insertSQL =
				"INSERT INTO ad_stat " +
				"VALUES(?,?,?,?)";
		List<Object[]> insertParamList = new ArrayList<>();
		for (AdStat adStat : insertAdStats) {
			Object[] params = new Object[]{
					adStat.getDate(),
					adStat.getProvince(),
					adStat.getCity(),
					adStat.getAdid(),
					adStat.getClickCount()
			};
			insertParamList.add(params);
		}
		jdbcHelper.executeBatch(insertSQL, insertParamList);

	}
}
