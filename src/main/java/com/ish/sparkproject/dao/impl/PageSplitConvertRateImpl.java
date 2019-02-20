package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.IPageSplitConvertRateDAO;
import com.ish.sparkproject.domain.PageSplitConvertRate;
import com.ish.sparkproject.jdbc.JDBCHelper;

public class PageSplitConvertRateImpl implements IPageSplitConvertRateDAO {
	@Override
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";
		Object[] params = new Object[]{
				pageSplitConvertRate.getTaskid(),
				pageSplitConvertRate.getConvertRate()
		};
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}
