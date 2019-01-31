package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.ISessionDetailDAO;
import com.ish.sparkproject.domain.SessionDetail;
import com.ish.sparkproject.jdbc.JDBCHelper;

public class SessionDetailDAOImpl implements ISessionDetailDAO {
	@Override
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[]{
				sessionDetail.getTaskid(),
				sessionDetail.getUserid(),
				sessionDetail.getSessionid(),
				sessionDetail.getPageid(),
				sessionDetail.getActionTime(),
				sessionDetail.getSearchKeyword(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}
