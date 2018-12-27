package com.ish.sparkproject.jdbcTest;

import java.sql.*;

public class JdbcCRUD {

	public static void main(String[] args) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/learn",
					"root",
					"1234");
			String sql = "insert into test1(name,age) values (?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1,",喵");
			pstmt.setInt(2,3);
			int rel = pstmt.executeUpdate();
			System.out.println("执行结果影响了["+rel+"]行数据");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				pstmt.close();
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}
}
