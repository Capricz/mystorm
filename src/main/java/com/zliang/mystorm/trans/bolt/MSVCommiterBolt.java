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

public class MSVCommiterBolt extends BaseTransactionalBolt implements ICommitter {
	
	static Logger logger = Logger.getLogger(MSVCommiterBolt.class);
	
	public static final String LAST_COMMITED_TRANSACTION_FIELD = "LAST_COMMIT";
	TransactionAttempt id;
	BatchOutputCollector collector;
	MongoManager mongoManager;
	VerticaManager verticaManager;
	
//	List<ObjectId> rowIds;
	String[] fields;
	List<DBObject> batchRows;
	String collectionName;
	
//	HashMap<String, Long> hashtags = new HashMap<String, Long>();
//	HashMap<String, Long> users = new HashMap<String, Long>();
//	HashMap<String, Long> usersHashtags = new HashMap<String, Long>();
	
	public MSVCommiterBolt(){
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context,BatchOutputCollector collector, TransactionAttempt id) {
		logger.debug("in MSVCommiterBolt - prepare");
		this.id = id;
		this.collector = collector;
		mongoManager = new MongoManager();
		verticaManager = new VerticaManager();
		fields = mongoManager.getCollectionFields();
		logger.debug("in MSVCommiterBolt - instance batchRows...");
		batchRows = new ArrayList<>();
	}

	@Override
	public void execute(Tuple tuple) {
		logger.debug("in MSVCommiterBolt - execute");
		List<Object> values = tuple.getValues();
		TransactionAttempt tx = (TransactionAttempt) tuple.getValueByField("txid");
		String collectionName = (String) tuple.getValueByField("collection");
		List<DBObject> rows =  (List<DBObject>) tuple.getValueByField("rows");
		if(rows!=null && !rows.isEmpty()){
			batchRows.addAll(rows);
			try {
				logger.debug("add row to vertica batch");
				for (DBObject row : rows) {
					collectionName = collectionName;
					verticaManager.addRowToBatch(row);
				}
				verticaManager.executeBatchRows();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.debug(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
			}
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
		 /*finally{
			verticaManager.closePreparedStatment();
		}*/
//			logger.debug("batch update complete, batchRows size : "+batchRows.size());
//		List<DBObject> batchRows = mongoManager.updateFlagToComplete(this.batchRows);
		if(batchRows!=null && !batchRows.isEmpty()){
			mongoManager.updateFlagToComplete(collectionName,batchRows);
		}
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
