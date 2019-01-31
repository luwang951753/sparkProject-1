package com.ish.sparkproject.dao.impl;

import com.ish.sparkproject.dao.ISessionAggrStatDAO;
import com.ish.sparkproject.domain.SessionAggrStat;
import com.ish.sparkproject.jdbc.JDBCHelper;

/**
 * session聚合统计DAO实现类
 */
public class SessionAggrStatDAOImpl implements ISessionAggrStatDAO {
	@Override
	public void insert(SessionAggrStat sessionAggrStat) {
		String sql = "insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//		System.out.println(sessionAggrStat.toString());
		Object[] params = new Object[]{
				sessionAggrStat.getTaskid(),
				sessionAggrStat.getSession_count(),
				sessionAggrStat.getVisitLength_1s_3s(),
				sessionAggrStat.getVisitLength_4s_6s(),
				sessionAggrStat.getVisitLength_7s_9s(),
				sessionAggrStat.getVisitLength_10s_30s(),
				sessionAggrStat.getVisitLength_30s_60s(),
				sessionAggrStat.getVisitLength_1m_3m(),
				sessionAggrStat.getVisitLength_3m_10m(),
				sessionAggrStat.getVisitLength_10m_30m(),
				sessionAggrStat.getVisitLength_30m(),
				sessionAggrStat.getStepLength_1_3(),
				sessionAggrStat.getStepLength_4_6(),
				sessionAggrStat.getStepLength_7_9(),
				sessionAggrStat.getStepLength_10_30(),
				sessionAggrStat.getStepLength_30_60(),
				sessionAggrStat.getStepLength_60()
		};

		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}
