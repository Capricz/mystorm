package com.zliang.mystorm.vertica;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class RefreshSyncUpdateTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String url = "jdbc:vertica://C0045453.itcs.hp.com:5433/Vertica";
		String username = "dbadmin";
		String password = "vertica123";
		String sql = " TRUNCATE TABLE SYNCUPDATE ";
		
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = DriverManager.getConnection(url, username, password);
			stmt = conn.createStatement();
			stmt.execute(sql);
			System.out.println("truncate table complete");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

}
