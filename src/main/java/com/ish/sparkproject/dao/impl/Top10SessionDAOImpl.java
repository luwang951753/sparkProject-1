package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.ITop10SessionDAO;
import com.ish.sparkproject.domain.Top10Session;
import com.ish.sparkproject.jdbc.JDBCHelper;

public class Top10SessionDAOImpl implements ITop10SessionDAO {
	@Override
	public void insert(Top10Session top10Session) {
		String sql = "insert into top10_session values(?,?,?,?)";
		Object[] params = new Object[]{
				top10Session.getTaskid(),
				top10Session.getCategoryid(),
				top10Session.getSessionid(),
				top10Session.getClickCount()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}
