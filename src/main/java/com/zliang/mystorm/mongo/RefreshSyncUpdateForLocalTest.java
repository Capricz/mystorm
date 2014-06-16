package com.zliang.mystorm.mongo;

import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.Calendar;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.zliang.mystorm.util.Const;

public class RefreshSyncUpdateForLocalTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Mongo mongoClient = null;
		DB db = null;
//		String collectionName = "SyncUpdate";
		int size = 1000;
		Calendar cal = Calendar.getInstance();
		cal.set(2014, 0, 1, 12, 22, 30);
		try {
			mongoClient = new Mongo( "localhost" , 27017 );
			
			db = mongoClient.getDB("mydb");
			
			DBCollection collection1 = db.getCollection(Const.COLLECTION_NAME);
			DBCollection collection2 = db.getCollection(Const.COLLECTION_NAME2);;
			
			if(collection1.count()!=0){
				collection1.drop();
				collection2.drop();
				System.out.println("exist dirty data, removed...");
				collection1 = db.getCollection(Const.COLLECTION_NAME);
			}
			
			for (int i = 0; i < size; i++) {
				
				BasicDBObject row = new BasicDBObject();
				row.put("group", "group"+i);
				row.put("name","name"+i);
				row.put("time", new Timestamp(cal.getTimeInMillis()));
				row.put("flag", 0);
				
				cal.add(Calendar.DATE, 1);
				collection1.save(row);
				collection2.save(row);
			}
			System.out.println("insert complete successfully");
			
			printCollection(collection1);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
			mongoClient.close();
		}
	}

	private static void printCollection(DBCollection collection) {
		DBCursor cursor = collection.find();
		for(;cursor.hasNext();){
			DBObject row = cursor.next();
			System.out.println(row);
		}
	}

}
