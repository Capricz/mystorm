package com.zliang.mystorm.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.vertica.jdbc.VerticaConnection;

public class VerticaBolt implements IRichBolt {
	
	OutputCollector _collector;
	
	Connection conn;
	PreparedStatement pstmt;
	final String host = "localhost";
	final int port = 5433;
	final String database = "test";
	final String url = "jdbc:vertica://"+host+":"+port+"/"+database;
	final String tableName = "testTbl";
	final String sql = " insert into "+ tableName+"(objId,ts,d,username,password) values(?,?,?,?,?)";

	boolean _isFinished = false;
	long interval = 1000 * 2;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		_collector = collector;
		Properties myProp = new Properties();
        myProp.put("user", "dbadmin");
        myProp.put("password", "password");
        try {
			conn = DriverManager.getConnection(url,myProp);
			((VerticaConnection)conn).setProperty("DirectBatchInsert", true);
		} catch (SQLException | NullPointerException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		if(!_isFinished){
			Utils.sleep(interval);
		}
		try {
			pstmt = conn.prepareStatement(sql);
			ObjectId objId = (ObjectId) input.getValue(0);
			String objIdStr = objId.toStringMongod();
			BSONTimestamp ts = (BSONTimestamp) input.getValue(1);
			Timestamp timestamp = new Timestamp(ts.getTime());
			Date d = (Date) input.getValue(2);
			String user = (String) input.getValue(3);
			String pass = (String) input.getValue(4);
			
			pstmt.setString(1, objIdStr);
			pstmt.setTimestamp(2, timestamp);
			pstmt.setDate(3, new java.sql.Date(d.getTime()));
			pstmt.setString(4, user);
			pstmt.setString(5, pass);
			
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			try {
				if(pstmt!=null){
					pstmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		_isFinished = true;
		_collector.ack(input);
	}

	@Override
	public void cleanup() {
		try {
			if(conn!=null){
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
