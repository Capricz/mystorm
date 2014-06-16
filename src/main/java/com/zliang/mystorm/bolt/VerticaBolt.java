package com.zliang.mystorm.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.esotericsoftware.minlog.Log;
import com.mongodb.BasicDBObject;
import com.vertica.jdbc.VerticaConnection;
import com.zliang.mystorm.spout.MongoSpout;

public class VerticaBolt implements IRichBolt {
	
	public static Logger log = LoggerFactory.getLogger(VerticaBolt.class);
	
	OutputCollector _collector;
	
	Connection conn;
	PreparedStatement pstmt;
	final String host = "C0045453.itcs.hp.com";
	final int port = 5433;
	final String database = "Vertica";
	final String url = "jdbc:vertica://"+host+":"+port+"/"+database;
	final String tableName = "PATEST";
	final String sql = " insert into "+ tableName+"(objId,ts,d,username,password) values(?,?,?,?,?)";

	boolean _isFinished = false;
	long interval = 1000 * 2;
	String[] columnNames;
	final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	DateFormat sdf;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		_collector = collector;
		columnNames = new String[]{"_id",
									"appId",
									"browserName",
									"date",
									"env",
									"group",
									"language",
									"location",
									"machine-id",
									"offset",
									"page",
									"referrer",
									"srcElement",
									"time",
									"timezone",
									"tracking-id",
									"type",
									"userAgent"};
		Properties myProp = new Properties();
        myProp.put("user", "dbadmin");
        myProp.put("password", "vertica123");
        sdf = new SimpleDateFormat(DATE_FORMAT);
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
			if(columnNames!=null && columnNames.length>0){
				String sql = " INSERT INTO "+tableName+" ";
				StringBuffer sbColumn = new StringBuffer();
				StringBuffer sbValue = new StringBuffer();
				
				for (int i = 0; i < columnNames.length; i++) {
					String columnName;
					String field = columnNames[i];
					//for filter '-', e.g. tracking-id -> trackingId
					field = field.replaceAll("-", "");
					Object object = input.getValueByField(field);
					sbColumn.append(field+",");
					
					//remove last comma,append sql for start of values
					if(i==columnNames.length-1){
						sbColumn.deleteCharAt(sbColumn.length()-1);
						sbColumn.append(") VALUES(");
					}
					
					sbColumn.append("?,");
					
					//remove last comma,append sql for end of values
					if(i==columnNames.length-1){
						sbColumn.deleteCharAt(sbColumn.length()-1);
						sbColumn.append(")");
					}
				}
				
				sql += sbColumn.toString();
				
				log.info("insert table: "+tableName+", sql : "+sql);
				
				for (int i = 0; i < columnNames.length; i++) {
					String field = columnNames[i];
					//for filter '-', e.g. tracking-id -> trackingId
					field = field.replaceAll("-", "");
					Object object = input.getValueByField(field);
					if(object instanceof ObjectId){
						ObjectId objectId = (ObjectId)object;
						pstmt.setString(i, objectId.toString());
					} else if(object instanceof Date){
						Date date = (Date) object;
						pstmt.setString(i, sdf.format(date));
					} else if(object instanceof BSONTimestamp){
						BSONTimestamp time = (BSONTimestamp)object;
						pstmt.setTimestamp(i, new Timestamp(time.getTime()));
					} else if(object instanceof String){
						String value = (String) object;
						pstmt.setString(i, value);
					}else if(object instanceof BasicDBObject){
						BasicDBObject dbObj = (BasicDBObject)object;
//						Set<Entry<String,Object>> entrySet = dbObj.entrySet();
						pstmt.setString(i, dbObj.toString());
					}
				}
				
			}
			
			
			
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
