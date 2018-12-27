package com.ish.sparkproject.jdbc;

import com.ish.sparkproject.conf.ConfigurationManager;
import com.ish.sparkproject.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * JDBC辅助组件
 */
public class JDBCHelper {
	private static JDBCHelper instance = null;

	static{
		try {
			// Step1.加载数据库的驱动
			Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 	Step2.设计JDBChelper类的单例模式,
	 *  	为了保证JDBCHelper类中封装的连接池(datasource)有且仅有一个
	 *  	所以要通过单例模式确保JDBChelper类的实例化对象有且仅有一个
	 */
	public static JDBCHelper getInstance(){
		if(instance == null){
			synchronized (JDBCHelper.class) {
				if (instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}

	// 数据库连接池实例化
	private LinkedList<Connection> datasource = new LinkedList<Connection>();

	/*
	 *	Step3.在私有化构造的过程中,将创建连接池,并将一定数量的connection放入池子中
	 */
	private JDBCHelper(){
		// 确认数据库连接池中connection的个数
		int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
		String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
		String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
		String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
		// 创建指定数量的数据库连接,并放入数据库连接池中
		for (int i = 0; i < datasourceSize; i++) {
			try {
				Connection conn = DriverManager.getConnection(url, user, password);
				datasource.push(conn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 *	Step4.提供获取数据库连接的方法
	 *		有可能，你去获取的时候，这个时候，连接都被用光了，你暂时获取不到数据库连接
	 * 		所以我们要自己编码实现一个简单的等待机制，去等待获取到数据库连接
	 */
	public synchronized Connection getConnection(){
		while (datasource.size() == 0) {
			try {
				Thread.sleep(10);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}

	/*
	 * Step5.开发数据库的CRUD方法
	 * 		5.1 设计 增删改SQL语句的方法
	 * 		5.2 设计 查询SQL语句的方法
	 * 		5.3 设计 批量执行SQL语句的方法
	 */

	/**
	 * 增删改SQL语句的方法
	 * @param sql 查询语句
	 * @param params
	 * @return SQL语句结果所影响的行数
	 */
	public int executeUpdate(String sql, Object[] params){
		int rtn = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		try{
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				pstmt.setObject(i+1, params[i]);
			}
			rtn =  pstmt.executeUpdate();
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			if (conn != null){
				datasource.push(conn);
			}
		}
		return rtn ;
	}

	/**
	 * 询SQL语句的方法
	 * @param sql
	 * @param params
	 * @param callback
	 */
	public void executeQuery(String sql, Object[] params, QureyCallback callback){
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				pstmt.setObject(i+1, params[i]);
			}
			rs = pstmt.executeQuery();
			callback.process(rs);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			if (conn != null){
				datasource.push(conn);
			}
		}
	}

	/**
	 * 批量执行SQL语句的方法
	 *
	 * 批量执行SQL语句，是JDBC中的一个高级功能
	 * 默认情况下，每次执行一条SQL语句，就会通过网络连接，向MySQL发送一次请求
	 *
	 * 但是，如果在短时间内要执行多条结构完全一模一样的SQL，只是参数不同
	 * 虽然使用PreparedStatement这种方式，可以只编译一次SQL，提高性能，但是，还是对于每次SQL
	 * 都要向MySQL发送一次网络请求
	 *
	 * 可以通过批量执行SQL语句的功能优化这个性能
	 * 一次性通过PreparedStatement发送多条SQL语句，比如100条、1000条，甚至上万条
	 * 执行的时候，也仅仅编译一次就可以
	 * 这种批量执行SQL语句的方式，可以大大提升性能
	 *
	 * @param sql
	 * @param paramsList
	 * @return 每条SQL语句影响的行数
	 */
	public int[] executeBatch(String sql, List<Object[]> paramsList){
		int[] rtn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = getConnection();
			// 1.取消自动提交
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			// 2.遍历每个参数数组,将每组参数设置到SQL语句中,然后addBatch()打包!
			for (Object[] params : paramsList) {
				for (int i = 0; i < params.length ; i++) {
					pstmt.setObject(i+1, params[i]);
				}
				pstmt.addBatch();
			}
			// 3.使用PreparedStatement.executeBatch(),执行批量SQL语句
			pstmt.executeBatch();
			// 4.使用commit()提交查询语句
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if (conn != null){
				datasource.push(conn);
			}
		}

		return rtn;
	}


	/**
	 * 内部查询回调接口,针对不同处理结果,提供一个统一的接口方便覆写
	 */
	public static interface QureyCallback{
		/**
		 * 处理查询的结果ResultSet类
		 * @param rs
		 * @throws Exception
		 */
		void process(ResultSet rs) throws Exception;
	}
}
