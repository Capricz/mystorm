package com.zliang.mystorm.trans.bolt;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.mongodb.DBObject;
import com.zliang.mystorm.trans.dao.MongoManager;
import com.zliang.mystorm.trans.dao.VerticaManager;
import com.zliang.mystorm.util.Const;

public class MSVCommiterBatchBolt extends BaseTransactionalBolt implements ICommitter {
	
	static Logger logger = Logger.getLogger(MSVCommiterBatchBolt.class);
	
	public static final String LAST_COMMITED_TRANSACTION_FIELD = "LAST_COMMIT";
	TransactionAttempt id;
	BatchOutputCollector collector;
	MongoManager mongoManager;
	VerticaManager verticaManager;
	
//	List<ObjectId> rowIds;
//	String[] fields;
	List<DBObject> batchRows;
	String collectionName;
	
//	HashMap<String, Long> hashtags = new HashMap<String, Long>();
//	HashMap<String, Long> users = new HashMap<String, Long>();
//	HashMap<String, Long> usersHashtags = new HashMap<String, Long>();
	
	public MSVCommiterBatchBolt(){
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context,BatchOutputCollector collector, TransactionAttempt id) {
		logger.debug("in MSVCommiterBatchBolt - prepare");
		this.id = id;
		this.collector = collector;
		mongoManager = new MongoManager();
		verticaManager = new VerticaManager();
//		fields = mongoManager.getCollectionFields();
		logger.debug("in MSVCommiterBolt - instance batchRows...");
		batchRows = new ArrayList<>();
	}

	@Override
	public void execute(Tuple tuple) {
		logger.debug("in MSVCommiterBolt - execute");
//		List<Object> values = tuple.getValues();
		TransactionAttempt tx = (TransactionAttempt) tuple.getValueByField("txid");
		logger.debug("txid : "+tx.getTransactionId());
		String collection = (String) tuple.getValueByField("collection");
		logger.debug("collection : "+collection);
		DBObject row =  (DBObject) tuple.getValueByField("row");
		logger.debug("receive row: "+row);
		if(row!=null){
			Integer flag = (Integer) row.get(Const.FIELD_FLAG);
			logger.debug("row flag : "+flag);
			if(Const.STATUS_IN_PROGRESS == flag){
				collectionName = collection;
				logger.debug("adding row to batchRows");
				batchRows.add(row);
			} else{
				logger.debug(" row flag is invalid, flag : "+flag);
			}
		} else{
			logger.error("row is empty");
		}
		
		/*for (int i = 0; i < values.size(); i++) {
			if(i==1){
				ObjectId _id = (ObjectId) values.get(i);
			}
		}*/
		/*String origin = tuple.getSourceComponent();
		if ("users-splitter".equals(origin)) {
			String user = tuple.getStringByField("user");
			count(users, user, 1);
		} else if ("hashtag-splitter".equals(origin)) {
			String hashtag = tuple.getStringByField("hashtag");
			count(hashtags, hashtag, 1);
		} else if ("user-hashtag-merger".equals(origin)) {
			String hashtag = tuple.getStringByField("hashtag");
			String user = tuple.getStringByField("user");
			String key = user + ":" + hashtag;
			Integer count = tuple.getIntegerByField("count");
			count(usersHashtags, key, count);
		}*/
		
	}

	/*private void count(HashMap<String, Long> map, String key, int count) {
		Long value = map.get(key);
		if (value == null)
			value = (long) 0;
		value += count;
		map.put(key, value);
	}*/

	@Override
	public void finishBatch() {
		if(batchRows==null || batchRows.isEmpty()){
			logger.error("batchRows is empty, couldn't insert row to vertica DB");
			return;
		} else{
			logger.debug("current batchRows size : "+batchRows.size());
		}
		try {
			logger.debug("add row to vertica batch");
			verticaManager.generatePreparedStatement(collectionName,batchRows.get(0));
			for (DBObject row : batchRows) {
				verticaManager.addRowToBatch(row);
			}
			verticaManager.executeBatchRows();
			mongoManager.updateFlagToComplete(collectionName,batchRows);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
		} /*finally{
			batchRows.clear();
		}*/
//			logger.debug("batch update complete, batchRows size : "+batchRows.size());
//		List<DBObject> batchRows = mongoManager.updateFlagToComplete(this.batchRows);
		
		/*
		BigInteger transactionId = id.getTransactionId();
		String lastCommitedTransaction = jedis.get(LAST_COMMITED_TRANSACTION_FIELD);
		String currentTransaction = "" + id.getTransactionId();
		if (currentTransaction.equals(lastCommitedTransaction))
			return;
		Transaction multi = jedis.multi();
		multi.set(LAST_COMMITED_TRANSACTION_FIELD, currentTransaction);
		Set<String> keys = hashtags.keySet();
		for (String hashtag : keys) {
			Long count = hashtags.get(hashtag);
			multi.hincrBy("hashtags", hashtag, count);
		}
		keys = users.keySet();
		for (String user : keys) {
			Long count = users.get(user);
			multi.hincrBy("users", user, count);
		}
		keys = usersHashtags.keySet();
		for (String key : keys) {
			Long count = usersHashtags.get(key);
			multi.hincrBy("users_hashtags", key, count);
		}
		multi.exec();*/
		
	}

	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
