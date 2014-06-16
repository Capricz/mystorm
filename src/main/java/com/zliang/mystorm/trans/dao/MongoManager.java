package com.zliang.mystorm.trans.dao;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
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
import com.zliang.mystorm.trans.model.ResultSetDTO;
import com.zliang.mystorm.trans.spout.AbstractMongoManager;
import com.zliang.mystorm.util.Const;
import com.zliang.mystorm.util.Util;

public class MongoManager extends AbstractMongoManager {
	
	static Logger logger = Logger.getLogger(MongoManager.class);
	
//	public static final int MAX_TRANSACTION_SIZE = 1;
	
	public static final int FETCH_BATCH_SIZE = 2;
	
	public static final String COLLECTION_NAMES = "SyncUpdate,SyncUpdate2";

	public static final String SEPARATOR = ",";

	private Mongo mongoClient;
	private DB db;
//	private DBCollection collection;
//	DBObject query = new BasicDBObject(Const.FIELD_FLAG, Const.STATUS_DRAFT);
	DBObject query = new BasicDBObject().append(Const.FIELD_FLAG, -1);
//	BasicDBObject or = new BasicDBObject();
//	List<BasicDBObject> or = new ArrayList<>();
//	
//	DBObject query = new BasicDBObject().append(Const.FIELD_FLAG, new BasicDBObject("$or",or));
//	query = new BasicDBObject("$or",null);
	DBObject sort = new BasicDBObject(Const.FIELD_TIME, Const.SORT_ORDER_ASC);
	
	public static void main(String[] args) throws UnknownHostException {
		MongoManager m = new MongoManager();
//		List<DBObject> list = m.fetchAndUpdateAvailableToRead();
//		printSelectedRows(list);
//		m.close();
//		List<ObjectId> rowIds = Util.convertToIdList(list);
//		printRowIds(rowIds);
	}
	
	private static void printRowIds(List<ObjectId> rowIds) {
		for (ObjectId objectId : rowIds) {
			System.out.println("_id:"+objectId);
		}
	}

	public MongoManager(){
		logger.debug("in MongoManager - constructor");
		try {
			initialize();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public void initialize() throws UnknownHostException {
		logger.info("in MongoManager - initialize");
		mongoClient = new Mongo( "localhost" , 27017 );
		logger.info("in MongoManager - new mongoClient...");
		db = mongoClient.getDB("mydb");
		logger.info("in MongoManager - new db...");
		BasicDBList or = new BasicDBList();
		or.add(new BasicDBObject(Const.FIELD_FLAG,new BasicDBObject("$exists",false)));
		or.add(new BasicDBObject(Const.FIELD_FLAG,Const.STATUS_DRAFT));
		query = new BasicDBObject("$or",or);
//		collection = db.getCollection(Const.COLLECTION_NAME);
//		logger.info("in MongoManager - new collection...");
//		printCollection(collection);
	}
	
	private static void printSelectedRows(List<DBObject> list) {
		if(list!=null && !list.isEmpty()){
			for (DBObject row : list) {
				System.out.println(row);
			}
		} else{
			System.out.println(" list is empty");
		}
		
	}

	private static void printCollection(DBCollection collection) {
		if(collection==null || collection.count()==0l){
			System.out.println("collection:"+collection+" is empty.");
			return;
		}
		DBCursor cursor = collection.find();
		for(;cursor.hasNext();){
			DBObject row = cursor.next();
			System.out.println(row);
		}
	}
	

	/*@Override
	public boolean isAvailableToRead() {
		List<DBObject> selectedRows = fetchAvailableToRead();
		return !selectedRows.isEmpty();
	}*/

	/*public List<DBObject> fetchAvailableToRead() {
		List<DBObject> rows = new ArrayList<>();
//		query.put(Const.FIELD_FLAG, Const.STATUS_DRAFT);
//		sort.put(Const.FIELD_TIME, Const.SORT_ORDER_ASC);
		DBCursor cursor = null;
		try {
			cursor = collection.find(query).sort(sort).limit(MAX_TRANSACTION_SIZE);
			while(cursor.hasNext()){
				DBObject row = cursor.next();
				logger.debug("in fetchAvailableRows : "+row);
				rows.add(row);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(cursor!=null){
				cursor.close();
			}
		}
		logger.info("fetch collection:"+collection.getName()+", rows size : "+rows.size());
		return rows;
	}*/

	@Override
	public long getNextRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNextWrite() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() {
		logger.info("in MongoManager - close mongoClient...");
		mongoClient.close();
	}

	@Override
	public void setNextRead(long nextRead) {
		// TODO Auto-generated method stub
		
	}

	/*public List<DBObject> updateFlagToInprogress(List<DBObject> batchRows) {
		DBObject setValue = new BasicDBObject();
		setValue.put("flag", 1);
		for (DBObject row : batchRows) {
			row.put(Const.FIELD_FLAG, Const.STATUS_IN_PROGRESS);
			collection.save(row);
		}
		logger.debug("update rows, rowId:"+Util.convertDBObjectListToString(batchRows));
		return batchRows;
	}*/
	
	public List<DBObject> updateFlagToComplete(String collectionName,List<DBObject> batchRows) {
		if(batchRows!=null && !batchRows.isEmpty()){
			if(db.collectionExists(collectionName)){
				DBCollection collection = db.getCollection(collectionName);
				DBObject setValue = new BasicDBObject();
				setValue.put("flag", 1);
				for (DBObject row : batchRows) {
					row.put(Const.FIELD_FLAG, Const.STATUS_COMPLETED);
					collection.save(row);
				}
			}
		} else{
			logger.error("batchRows is empty");
		}
		
		return batchRows;
	}

	public List<DBObject> fetchAndUpdateAvailableToRead(String collectionName,List<ObjectId> rowIds) {
		List<DBObject> availableRows = new ArrayList<>();
		db.requestStart();
		db.requestEnsureConnection();
		try {
			/*List<DBObject> availableRows = fetchAvailableRows();
			if(availableRows!=null && !availableRows.isEmpty()){
				availableRows = updateFlagToInprogress(availableRows);
//			db.requestDone();
				return availableRows;
			} else{
				logger.error("fetchAndUpdateAvailableToRead is empty");
				return new ArrayList<DBObject>();
			}*/
			DBObject idQuery = new BasicDBObject(Const.FIELD_ID,new BasicDBObject("$in",rowIds));
			DBObject fields = null;
			DBObject update = new BasicDBObject("$set",new BasicDBObject(Const.FIELD_FLAG,1));
			boolean remove = false;
//			DBObject update = new BasicDBObject();
			boolean returnNew = true;
			boolean upsert = false;
			DBCollection collection = db.collectionExists(collectionName)?db.getCollection(collectionName):null;
			if(collection == null){
				throw new Exception("collection:"+collection+" not found");
			}
			DBObject updatedRow = collection.findAndModify(idQuery, fields, sort, remove, update, returnNew, upsert);
			if(updatedRow!=null){
				availableRows.add(updatedRow);
			} else{
				logger.warn("record can't be fetched");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			db.requestDone();
		}
		return availableRows;
	}
	
	public ResultSetDTO fetchAndUpdateAvailableToRead(String previousCollectionName) {
		ResultSetDTO dto = new ResultSetDTO();
		boolean hasAvailabeDataInCollection = false;
		String collectionName = "";
		List<DBObject> availableRows = new ArrayList<>();
		if(COLLECTION_NAMES!=null){
			String[] collectionArr = COLLECTION_NAMES.split(SEPARATOR);
			if(collectionArr.length>0){
				collectionArr = Util.reorgenizeCollectionArr(previousCollectionName,collectionArr);
				db.requestStart();
				db.requestEnsureConnection();
				DBCursor cursor = null;
				for (int i = 0; i < collectionArr.length; i++) {
					collectionName = collectionArr[i];
					if(db.collectionExists(collectionName)){
						DBCollection collection = db.getCollection(collectionName);
						logger.info("retrieve collection : "+collectionName);
//						DBObject idQuery = new BasicDBObject(Const.FIELD_ID,new BasicDBObject("$in",rowIds));
						DBObject fields = null;
						DBObject update = new BasicDBObject("$set",new BasicDBObject(Const.FIELD_FLAG,Const.STATUS_IN_PROGRESS));
						boolean remove = false;
//						DBObject update = new BasicDBObject();
						boolean returnNew = true;
						boolean upsert = false;
						boolean multi = false;
						cursor = collection.find(query).sort(sort).batchSize(FETCH_BATCH_SIZE);
						while(cursor.hasNext()){
							DBObject row = cursor.next();
							
							ObjectId id = (ObjectId) row.get(Const.FIELD_ID);
							DBObject in = new BasicDBObject(Const.FIELD_ID,id).append(Const.FIELD_FLAG, new BasicDBObject("$ne",Const.STATUS_IN_PROGRESS));
							WriteResult writeResult = collection.update(in, update, upsert, multi, WriteConcern.FSYNCED);
							if(writeResult.isUpdateOfExisting()){
								availableRows.add(row);
								if(!hasAvailabeDataInCollection){
									hasAvailabeDataInCollection = true;
								}
							} else{
								logger.info("id : "+row.get("_id")+" can not update successfully, flag : "+row.get(Const.FIELD_FLAG));
								continue;
							}
						}
						/*for (int j = 0; j < FETCH_BATCH_SIZE; j++) {
							DBObject updatedRow = collection.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
							if(updatedRow!=null){
								availableRows.add(updatedRow);
								if(!hasAvailabeDataInCollection){
									hasAvailabeDataInCollection = true;
								}
							} else{
								logger.debug("no record fetch, exit...");
								break;
							}
						}*/
						if(hasAvailabeDataInCollection){
							break;
						}
					} else{
						logger.warn("collection : "+collectionName + " does not exist in db["+db.getName()+"], skip this collection...");
						continue;
					}
				}
				db.requestDone();
			} else{
				logger.error("collection ["+COLLECTION_NAMES+"] is empty!");
			}
		} else{
			logger.error(" COLLECTION_NAMES is empty!");
		}
		dto.setBatchRows(availableRows);
		dto.setNotEmpty(hasAvailabeDataInCollection);
		dto.setCollectionName(collectionName);
		return dto;
	}
	
	

	public List<DBObject> getMessages(String collectionName, List<ObjectId> rowIds) {
		if(rowIds!=null){
			logger.debug("query rows by the given row ids : "+Util.convertObjectIdListToStr(rowIds));
		} else{
			logger.warn("rowIds is empty");
		}
		
		List<DBObject> selectRows = new ArrayList<>();
		DBCursor cursor = null;
		DBObject in = new BasicDBObject(Const.FIELD_ID,new BasicDBObject("$in",rowIds)).append(Const.FIELD_FLAG, Const.STATUS_IN_PROGRESS);
		try {
			if(db.collectionExists(collectionName)){
				DBCollection collection = db.getCollection(collectionName);
				cursor = collection.find(in); 
				while(cursor.hasNext()){
					selectRows.add(cursor.next());
				}
				logger.debug(" select rowCount :"+cursor.count());
			} else{
				String msg = "collection:"+collectionName+" not found!";
				logger.error(msg);
				throw new Exception(msg);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(cursor!=null){
				cursor.close();
			}
		}
		return selectRows;
	}

	public String[] getCollectionFields() {
		return Util.getCollectionField();
	}

}
