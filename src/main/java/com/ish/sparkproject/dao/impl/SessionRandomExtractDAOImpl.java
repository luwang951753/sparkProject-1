package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.ISessionRandomExtractDAO;
import com.ish.sparkproject.domain.SessionRandomExtract;
import com.ish.sparkproject.jdbc.JDBCHelper;

public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
	@Override
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		Object[] params = new Object[]{
				sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeywords(),
				sessionRandomExtract.getClickCategoryIds()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}

}
