package com.ish.sparkproject.dao;

import com.ish.sparkproject.domain.AreaTop3Product;

import java.util.List;

public interface IAreaTop3ProductDAO {
	void insertBatch(List<AreaTop3Product> areaTop3Products);
}
