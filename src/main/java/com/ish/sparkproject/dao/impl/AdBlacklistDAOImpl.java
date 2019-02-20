package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.IAdBlacklistDAO;
import com.ish.sparkproject.domain.AdBlacklist;
import com.ish.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AdBlacklistDAOImpl implements IAdBlacklistDAO {

	@Override
	public List<AdBlacklist> findAll() {
		String sql = "SELECT * FROM ad_blacklist";
		List<AdBlacklist> adBlacklists = new ArrayList<>();
		JDBCHelper.getInstance().executeQuery(sql, null, new JDBCHelper.QureyCallback() {
			@Override
			public void process(ResultSet rs) throws Exception {
				while (rs.next()){
					AdBlacklist adBlacklist = new AdBlacklist();
					long userid = Long.valueOf(String.valueOf(rs.getInt(1)));
					adBlacklist.setUserid(userid);
					adBlacklists.add(adBlacklist);
				}
			}
		});
		return adBlacklists;
	}

	@Override
	public void insertBatch(List<AdBlacklist> adBlacklists) {
		String sql =
				"INSERT INTO " +
					"ad_blacklist " +
				"VALUES" +
					"(?)";
		List<Object[]> paramsList = new ArrayList<>();
		for (AdBlacklist adBlacklist : adBlacklists) {
			Object[] params = new Object[]{
					adBlacklist.getUserid()
			};
			paramsList.add(params);
		}
		JDBCHelper.getInstance().executeBatch(sql, paramsList);
	}


}
