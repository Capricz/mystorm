package com.zliang.mystorm.spout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MySQLSpout implements IRichSpout{
	
	public static Logger log = LoggerFactory.getLogger(MySQLSpout.class);
	
	static final String url = "jdbc:mysql://localhost:3306/guessprice";
	static final String user = "guessprice";
	static final String pass = "guessprice";
	static final String driverName = "com.mysql.jdbc.Driver";
	static final String sql = "select * from user where userId = 10";
	SpoutOutputCollector _collector;
	boolean _isDistribute;
	Connection conn;
	
	/*public MySQLSpout(){
		this(true);
	}
	
	public MySQLSpout(boolean isDistribute){
		this._isDistribute = isDistribute;
	}*/

	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, user, pass);
			
//			columnNames = (List<String>) conf.get("columnNames");
//			columnTypes = (List<String>) conf.get("columnTypes");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this._collector = collector;
	}

	@Override
	public void close() {
		try {
			log.info("close connection...");
			if(!conn.isClosed()){
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		
		int userId = -1;
		String username = "";
		String password = "";
		String region = "";
		int roleId = -1;
		List<Object> tuple = new ArrayList<>();
		
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			
			StringBuffer sb = new StringBuffer();
			if(rs.next()) {
				userId = rs.getInt(1);
				username = rs.getString(2);
				password = rs.getString(3);
				region = rs.getString(4);
				roleId = rs.getInt(5);
				
//				sb.append("userId:"+userId);
//				sb.append(",username:"+username);
//				sb.append(",password:"+password);
//				sb.append(",region:"+region);
//				sb.append(",roleId:"+roleId+"\n");
				
				if(userId!=-1){
					tuple = new Values(userId,username,password,region,roleId);
				} else{
					log.info("rs is empty");
				}
			}else{
				log.info("no record for userid = 10 found, sleep");
				Utils.sleep(1000);
				return;
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		_collector.emit(tuple);
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("ack ###===>"+msgId);
	}

	@Override
	public void fail(Object msgId) {
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userId","username","password","region","roleId"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


}
