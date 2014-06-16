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
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.zliang.mystorm.util.Const;

public class ModifySyncUpdateForITGTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int limit = 5;
		limit = Math.abs(limit)*1;
		Mongo mongoClient = null;
		DB db = null;
		DBCollection collection = null;
		DBCursor cursor = null;
		
		try {
			
			mongoClient = new Mongo( "C0007294.itcs.hp.com" , 27017 );
			db = mongoClient.getDB("track");
			char[] pwdArr = {'a','d','m','i','n'};
			db.authenticate("admin", pwdArr);
			db.requestStart();
			db.requestEnsureConnection();
			collection = db.getCollection(Const.COLLECTION_NAME);
			
			BasicDBList or = new BasicDBList();
			or.add(new BasicDBObject("flag",new BasicDBObject("$exists",false)));
			or.add(new BasicDBObject("flag",0));
			
			BasicDBObject sort = new BasicDBObject();
			sort.put("time", 1);
			
			BasicDBObject notInProgress = new BasicDBObject();
			notInProgress.append("flag", new BasicDBObject("$ne",1));
			
			DBObject query = new BasicDBObject("$or",or);
//			query.put("flag", or);
			
			DBObject update = new BasicDBObject("$set",new BasicDBObject(Const.FIELD_FLAG,3));
			boolean upsert = false;
			boolean multi = false;
			cursor = collection.find(query).sort(sort).batchSize(limit);
			List<DBObject> fetchRows = new ArrayList<>();
			List<ObjectId> fetchIdList = new ArrayList<>();
			while(cursor.hasNext()){
				//fetchList.add(cursor.next());
				DBObject row = cursor.next();
				
				ObjectId id = (ObjectId) row.get("_id");
				DBObject in = new BasicDBObject(Const.FIELD_ID,id).append(Const.FIELD_FLAG, new BasicDBObject("$ne",Const.STATUS_IN_PROGRESS));
				WriteResult writeResult = collection.update(in, update, upsert, multi, WriteConcern.FSYNCED);
				if(writeResult.isUpdateOfExisting()){
					fetchIdList.add(id);
					fetchRows.add(row);
				}
			}
			
			System.out.println("fetchRows size : "+fetchRows.size());
			
			/*if(!fetchIdList.isEmpty()){
				DBObject in = new BasicDBObject(Const.FIELD_ID,new BasicDBObject("$in",fetchIdList)).append(Const.FIELD_FLAG, new BasicDBObject("$ne",Const.STATUS_IN_PROGRESS));
//				WriteResult writeResult = collection.update(in, update);
				WriteResult writeResult = collection.update(query, update, upsert, multi, WriteConcern.FSYNC_SAFE);
			}*/
			
			
			printCollection(collection);
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
//			cursor.close();
			db.requestDone();
			mongoClient.close();
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
