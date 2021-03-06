package com.zliang.mystorm.trans.spout;

import java.math.BigInteger;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

import com.mongodb.DBObject;
import com.zliang.mystorm.trans.dao.MongoManager;

public class MSVTransactionalSpoutEmitter implements ITransactionalSpout.Emitter<TransactionMetadata> {
	
	static Logger logger = Logger.getLogger(MSVTransactionalSpoutEmitter.class);

	MongoManager mongoManager;

	public MSVTransactionalSpoutEmitter() {
		logger.info("in MSVTransactionalSpoutEmitter - prepare to initialize mongoManager");
		mongoManager = new MongoManager();
	}

	@Override
	public void emitBatch(TransactionAttempt tx,TransactionMetadata coordinatorMeta, BatchOutputCollector collector) {
		logger.debug("in MSVTransactionalSpoutEmitter - emitBatch,txId:"+tx.getAttemptId());
//		mongoManager.setNextRead(coordinatorMeta.from + coordinatorMeta.quantity);
		String collectionName = coordinatorMeta.collectionName;
		List<DBObject> batchRows = mongoManager.getMessages(collectionName,coordinatorMeta.rowIds);
//		List<DBObject> batchRows = mongoManager.fetchAndUpdateAvailableToRead(coordinatorMeta.rowIds);
		if(batchRows!=null && !batchRows.isEmpty()){
			for (DBObject row : batchRows) {
				logger.debug("in MSVTransactionalSpoutEmitter - emit data,[txId:"+tx.getAttemptId()+",collection:"+collectionName+",row:"+row+"]");
				collector.emit(new Values(tx,collectionName,row));
			}
		} else{
			logger.error("couldn't fetch the record, will ignore emmit");
		}
		logger.debug("get rows, size : "+batchRows.size());
//		long tweetId = coordinatorMeta.from;
	}

	@Override
	public void cleanupBefore(BigInteger txid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		logger.info("in MSVTransactionalSpoutEmitter - close mongoManager");
		mongoManager.close();
	}

}
