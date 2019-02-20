package com.ish.sparkproject.dao;

import com.ish.sparkproject.domain.AdStat;

import java.util.List;

public interface IAdStatDAO {
	void updateBatch(List<AdStat> adStats);
}
