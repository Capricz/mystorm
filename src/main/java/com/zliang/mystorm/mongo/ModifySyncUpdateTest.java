package com.zliang.mystorm.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;
import com.zliang.mystorm.util.Const;

public class ModifySyncUpdateTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Mongo mongo = null;
		DB db = null;
		DBCollection collection = null;
		int limit = 1;
		
		try {
			
			mongo = new Mongo("localhost",27017);
			db = mongo.getDB("mydb");
			db.requestStart();
			db.requestEnsureConnection();
			collection = db.getCollection(Const.COLLECTION_NAME);
			
			
			
			/*if(cursor.hasNext()){
				System.out.println(cursor.next());
			}*/
			
			DBObject setValue = new BasicDBObject();
			setValue.put("flag", 1);
			DBCursor cursor = null;
			//0.generate query,sort
			DBObject query = new BasicDBObject();
//			query.append("name", "name1");
//			query.append("time", -1);
			query.put("flag", 0);
			BasicDBObject sort = new BasicDBObject();
			sort.put("time", 1);
			//1.retrieve first 2 record
			cursor = collection.find(query).sort(sort).limit(limit);
			List<DBObject> fetchList = new ArrayList<>();
			List<ObjectId> idList = new ArrayList<>();
			while(cursor.hasNext()){
				//fetchList.add(cursor.next());
				DBObject row = cursor.next();
				ObjectId id = (ObjectId) row.get(Const.FIELD_ID);
				idList.add(id);
			}
			//2.update record set flag to 1
			if(idList!=null && !idList.isEmpty()){
//				query = new BasicDBObject(Const.FIELD_ID,new BasicDBObject("$in",idList)).append(Const.FIELD_FLAG, 0);
				BasicDBObject query2 = new BasicDBObject().append(Const.FIELD_FLAG, 0);
				
				BasicDBList or = new BasicDBList();
//				or.add(new BasicDBObject("$eq",0));
				or.add(new BasicDBObject("flag",new BasicDBObject("$exists",false)));
				or.add(new BasicDBObject("flag",0));
				
				
				
				BasicDBList and = new BasicDBList();
				and.add(or);
				and.add(query);
				
				query2 = new BasicDBObject("$or",or);
				
//				BasicDBObject query2 = new BasicDBObject(Const.FIELD_FLAG, Const.STATUS_DRAFT);
				DBObject fields = null;
				DBObject update = new BasicDBObject("$set",new BasicDBObject(Const.FIELD_FLAG,1));
				boolean remove = false;
//				DBObject update = new BasicDBObject();
				boolean returnNew = true;
				boolean upsert = false;
//				WriteResult writeResult = collection.update(query, update);
				DBObject resultRow = collection.findAndModify(query2, fields, sort, remove, update, returnNew, upsert);
//				DBObject resultRow = collection.findAndModify(query, update);
				System.out.println(resultRow);
				System.out.println("########################################");
			}
			
			
//			DBObject updateQuery = new BasicDBObject("$set",setValue);
			
//			cursor = collection.find(query);
			
//			collection.update(query, updateQuery,false,true);
//			DBCursor cursor = collection.find();
//			cursor
			
//			collection.findAndModify(query, null, sort, false, setValue, false, false);
//			cursor = collection.find();
			
			/*while(cursor.hasNext()){
				DBObject row = cursor.next();
				row.put("flag", 1);
				collection.save(row);
//				System.out.println(row);
			}*/
			
			printCollection(collection);
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
			db.requestDone();
			mongo.close();
		}
	}

	private static void printCollection(DBCollection collection) {
		DBCursor cursor = collection.find();
		while(cursor.hasNext()){
			DBObject row = cursor.next();
			System.out.println(row);
		}
		
	}

}
