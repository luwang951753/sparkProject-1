package com.ish.sparkproject.dao.factory;

import com.ish.sparkproject.dao.*;
import com.ish.sparkproject.dao.impl.*;

/**
 * DAO工厂类,负责给实例化各个DAO的实现类提供实例化方法
 *
 */
public class DAOFactory {

	public static ITaskDAO getTaskDAOImpl(){
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAOImpl(){
		return new SessionAggrStatDAOImpl();
	}

	public static ISessionRandomExtractDAO getSessionRandomExtractDAOImpl(){
		return new SessionRandomExtractDAOImpl();
	}

	public static ISessionDetailDAO getSessionDetailDAOImpl(){
		return new SessionDetailDAOImpl();
	}

	public static ITop10CategoryDAO getTop10CategoryImpl(){
		return new Top10CategoryDAOImpl();
	}

	public static ITop10SessionDAO getTop10SessionDAOImpl(){
		return new Top10SessionDAOImpl();
	}

	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAOImpl(){
		return new PageSplitConvertRateImpl();
	}

	public static IAreaTop3ProductDAO getAreaTop3ProductDAOImpl(){
		return new AreaTop3ProductDAOImpl();
	}

	public static IAdUserClickCountDAO getAdUserClickCountDAOImpl(){
		return new AdUserClickCountDAOImpl();
	}

	public static IAdBlacklistDAO getAdBlacklistDAOImpl(){
		return new AdBlacklistDAOImpl();
	}

	public static IAdStatDAO getAdStatDAPImpl(){
		return new AdStatDAOImpl();
	}
}
