package com.zliang.mystorm.mongo;

import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

public class InsertSyncUpdateTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Mongo mongoClient = null;
		DB db = null;
		int size = 10;
		Calendar cal = Calendar.getInstance();
		cal.set(2014, 0, 1, 12, 22, 30);
		try {
//			mongoClient = new Mongo( "localhost" , 27017 );
//			db = mongoClient.getDB("mydb");
			
			mongoClient = new Mongo( "C0007294.itcs.hp.com" , 27017 );
			db = mongoClient.getDB("track");
			char[] pwdArr = {'a','d','m','i','n'};
			db.authenticate("admin", pwdArr);
			
			DBCollection syncUpdateColl = db.getCollection("SyncUpdate");
			
			for (int i = 0; i < size; i++) {
				
				BasicDBObject row = new BasicDBObject();
//				row.put("id", i);
				row.put("group", "group"+i);
				row.put("name","name"+i);
				row.put("time", new Timestamp(cal.getTimeInMillis()));
				row.put("flag", 0);
				
				cal.add(Calendar.DATE, 1);
				syncUpdateColl.save(row);
			}
			System.out.println("insert complete successfully");
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
			mongoClient.close();
		}
	}

}
