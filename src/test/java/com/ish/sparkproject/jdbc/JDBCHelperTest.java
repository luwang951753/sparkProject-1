package com.ish.sparkproject.jdbc;

import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCHelperTest {

	@Test
	public void executeUpdate() {
		String sql = "insert into test1(name,age) values(?,?)";
		Object[] params = {"wang", 23};
		int rel = JDBCHelper.getInstance().executeUpdate(sql, params);
		System.out.println("执行结果影响了["+rel+"]行数据");
	}

	@Test
	public void executeQuery() {
		// 定义一个Map接收查询结果
		final Map<String, Object> testRel = new HashMap<String, Object>();

		String sql = "select id,name,age from test1 where id = ?";
		Object[] params = {2};

		// 使用匿名内部类来覆写查询结果的处理方法(很高明的办法!!这里还可以使用java8的lamda表达式处理)
		JDBCHelper.QureyCallback callback = new JDBCHelper.QureyCallback() {
			@Override
			public void process(ResultSet rs) throws Exception {
				if(rs.next()){
					int id = rs.getInt(1);
					String name = rs.getString(2);
					int age = rs.getInt(3);
					// 匿名内部类如果要访问外部类的成员,比如方法内的局部变量,必须将局部变量定义为final类型,否则无法访问
					testRel.put("id", id);
					testRel.put("name", name);
					testRel.put("age", age);
				}
			}
		};
		JDBCHelper.getInstance().executeQuery(sql, params, callback);
		System.out.println("id:"+testRel.get("id")+"|"+"name:"+testRel.get("name")+"|"+"age:"+testRel.get("age"));
	}

	@Test
	public void executeBatch() {
		String sql = "insert into test1(name, age) values(?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		paramsList.add(new Object[]{"哈哈",31});
		paramsList.add(new Object[]{"宋智孝",26});
		paramsList.add(new Object[]{"gary",29});
		paramsList.add(new Object[]{"刘在石",35});
		int[] ints = JDBCHelper.getInstance().executeBatch(sql, paramsList);
	}
}