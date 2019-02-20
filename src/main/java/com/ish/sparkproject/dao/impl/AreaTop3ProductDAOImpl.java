package com.ish.sparkproject.dao.impl;


import com.ish.sparkproject.dao.IAreaTop3ProductDAO;
import com.ish.sparkproject.domain.AreaTop3Product;
import com.ish.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO {
	@Override
	public void insertBatch(List<AreaTop3Product> areaTop3Products) {
		String sql = "INSERT INTO area_top3_product values (?,?,?,?,?,?,?,?)";
		List<Object[]> paramsList = new ArrayList<>();
		for (AreaTop3Product areaTop3Product : areaTop3Products) {
			Object[] params = new Object[8];
			params[0] = areaTop3Product.getTaskid();
			params[1] = areaTop3Product.getArea();
			params[2] = areaTop3Product.getAreaLevel();
			params[3] = areaTop3Product.getProductid();
			params[4] = areaTop3Product.getCityInfos();
			params[5] = areaTop3Product.getClickCount();
			params[6] = areaTop3Product.getProductName();
			params[7] = areaTop3Product.getProductStatus();
			paramsList.add(params);
		}
		JDBCHelper.getInstance().executeBatch(sql, paramsList);

	}
}
