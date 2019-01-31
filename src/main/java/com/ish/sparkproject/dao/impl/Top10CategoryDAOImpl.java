package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.ITop10CategoryDAO;
import com.ish.sparkproject.domain.Top10Category;
import com.ish.sparkproject.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO {
	@Override
	public void insert(Top10Category top10Category) {
		String sql = "insert into top10_category values(?,?,?,?,?)";
		Object[] params = new Object[]{
				top10Category.getTaskid(),
				top10Category.getCategoryid(),
				top10Category.getClickCount(),
				top10Category.getOrderCount(),
				top10Category.getPayCount()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}
