package com.zliang.mystorm.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.zliang.mystorm.util.Const;

public class QuerySyncUpdateTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Mongo mongo = null;
		DB db = null;
		DBCollection collection = null;
		DBCursor cursor = null;
		BasicDBObject query = null;
		
		try {
			
			mongo = new Mongo("localhost",27017);
			db = mongo.getDB("mydb");
			
			collection = db.getCollection(Const.COLLECTION_NAME);
			
//			query = new BasicDBObject("flag", 2);
//			query = new BasicDBObject("flag2",new BasicDBObject("$exists",false));
//			BasicDBObject or = new BasicDBObject();
//			or.put("$exists",true);
			BasicDBList or = new BasicDBList();
			or.add(new BasicDBObject("flag",new BasicDBObject("$exists",false)));
			or.add(new BasicDBObject("flag",0));
			query = new BasicDBObject("$or",or);
			List<Integer> flagList = new ArrayList<>();
			flagList.add(1);
			flagList.add(0);
//			query = new BasicDBObject("flag",new BasicDBObject("$in",flagList));

			cursor = collection.find(query);

			DBCursor copyCursor = cursor.copy();
			if(cursor.hasNext()){
				int index = 0;
				while(cursor.hasNext()) {
				       System.out.println("["+index+"]"+cursor.next());
				       index++;
				       if(index == 2){
				    	   System.out.println(copyCursor.next());
				    	   break;
				       }
				}
			} else{
				System.out.println(" query can not find data");
			}
			/*DBCursor cursor = collection.find();
			for(;cursor.hasNext();){
				DBObject row = cursor.next();
				ObjectId _id = (ObjectId)row.get("_id");
				System.out.println("[id:"+_id+"]"+row);
			}*/
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
			cursor.close();
			mongo.close();
		}
	}
}
