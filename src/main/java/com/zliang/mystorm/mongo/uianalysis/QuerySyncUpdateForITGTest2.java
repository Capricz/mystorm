package com.zliang.mystorm.mongo.uianalysis;

import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.bson.types.BSONTimestamp;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.zliang.mystorm.util.Const;

public class QuerySyncUpdateForITGTest2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Mongo mongoClient = null;
		DB db = null;
		DBCollection collection = null;
		DBCursor cursor = null;
		
		try {
			
			mongoClient = new Mongo( "C0043223.itcs.hp.com" , 28989 );
			db = mongoClient.getDB("ui_tracking");
			char[] pwdArr = {'u','i','_','t','r','a','c','k','i','n','g'};
			db.authenticate("ui_tracking", pwdArr);
			
			collection = db.getCollection(Const.COLLECTION_NAME);
			
			// create our pipeline operations, first with the $match
			BasicDBList or = new BasicDBList();
			or.add(new BasicDBObject("flag",new BasicDBObject("$exists",false)));
			or.add(new BasicDBObject("flag",0));
			DBObject query = new BasicDBObject("$or",or);
			DBObject match = new BasicDBObject("$match", query);
//			DBObject match = new BasicDBObject("$match", new BasicDBObject("flag", 0));
			
			// build the $projection operation
			DBObject fields = new BasicDBObject("groupp", 1);
			fields.put("time", 1);
			fields.put("_id", 0);
			DBObject project = new BasicDBObject("$project", fields );
			
			// Now the $group operation
			DBObject groupFields = new BasicDBObject( "_id", "$groupp");
			groupFields.put("maxTime", new BasicDBObject( "$max", "$time"));
			DBObject group = new BasicDBObject("$group", groupFields);
			
			// Finally the $sort operation
			DBObject sort = new BasicDBObject("$sort", new BasicDBObject("time", -1));
			
			// run aggregation
			List<DBObject> pipeline = Arrays.asList(match, project, group, sort);
			AggregationOutput output = collection.aggregate(pipeline);
			
			DBObject retrieveRowsQuery = null;
			Calendar cal = Calendar.getInstance();
			cal.set(2014, 0, 8);
			Timestamp currTime = new Timestamp(cal.getTimeInMillis());
			BSONTimestamp bsonCurrTime = new BSONTimestamp();
			List<DBObject> batchRows = new ArrayList<>();
			
			DBObject sort1 = new BasicDBObject("$sort", new BasicDBObject("groupp", 1).append("time", -1));
//			DBObject sort1 = new BasicDBObject("$sort", new BasicDBObject("time", 1).append("groupp", 1));
			
			for (DBObject groupResult : output.results()) {
				String groupField = (String) groupResult.get("_id");
				Date time = (Date) groupResult.get("maxTime");
				if(time.before(currTime)){
					retrieveRowsQuery = query;
					retrieveRowsQuery.put("groupp", groupField);
					cursor = collection.find(retrieveRowsQuery).sort(sort1);
					while(cursor.hasNext()){
						DBObject row = cursor.next();
						batchRows.add(row);
					}
				}
			}
			
			for (DBObject row : batchRows) {
				System.out.println(row);
			}
			
			/*List<Integer> flagList = new ArrayList<>();
			flagList.add(1);
			flagList.add(0);
//			query = new BasicDBObject("flag",new BasicDBObject("$in",flagList));

			cursor = collection.find(query);

//			DBCursor copyCursor = cursor.copy();
			if(cursor.hasNext()){
				int index = 0;
				while(cursor.hasNext()) {
				       System.out.println("["+index+"]"+cursor.next());
				       index++;
//				       if(index == 2){
//				    	   System.out.println(copyCursor.next());
//				    	   break;
//				       }
				}
			} else{
				System.out.println(" query can not find data");
			}*/
			/*DBCursor cursor = collection.find();
			for(;cursor.hasNext();){
				DBObject row = cursor.next();
				ObjectId _id = (ObjectId)row.get("_id");
				System.out.println("[id:"+_id+"]"+row);
			}*/
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally{
//			cursor.close();
//			mongoClient.close();
		}
	}
}
