package com.ish.sparkproject.dao;

import com.ish.sparkproject.domain.AdBlacklist;

import java.util.List;

public interface IAdBlacklistDAO {

	void insertBatch(List<AdBlacklist> adBlacklists);

	List<AdBlacklist> findAll();
}
