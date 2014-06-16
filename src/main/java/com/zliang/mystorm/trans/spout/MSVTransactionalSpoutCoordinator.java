package com.zliang.mystorm.trans.spout;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.zliang.mystorm.trans.dao.MongoManager;
import com.zliang.mystorm.trans.model.ResultSetDTO;
import com.zliang.mystorm.util.Util;

public class MSVTransactionalSpoutCoordinator implements ITransactionalSpout.Coordinator<TransactionMetadata> {
	
	static Logger logger = Logger.getLogger(MSVTransactionalSpoutCoordinator.class);
	
	TransactionMetadata lastTransactionMetadata;
	MongoManager mongoManager;
//	long nextRead = 0;
//	long MAX_TRANSACTION_SIZE = 10;
	List<DBObject> nextBatchRows;
	String collectionName;
	String previousCollectionName;
	
	public MSVTransactionalSpoutCoordinator() throws UnknownHostException{
		logger.debug("in MSVTransactionalSpoutCoordinator - prepare to initialize mongo");
		mongoManager = new MongoManager();
//		nextRead = mongoManager.getNextRead();
		nextBatchRows = new ArrayList<>();
	}
	
	@Override
	public boolean isReady() {
//		boolean ready = mongoManager.isAvailableToRead();
		ResultSetDTO dto = mongoManager.fetchAndUpdateAvailableToRead(previousCollectionName);
		
		logger.info("ready to start a new transaction? : ["+(dto.isNotEmpty()?"Yes":"No")+"], fetch size : "+dto.getBatchRowsSize());
		if(dto.isNotEmpty()){
			
			nextBatchRows = dto.getBatchRows();
			collectionName = dto.getCollectionName();
			previousCollectionName = collectionName;
//			long idletime = 1 * 1000;
//			logger.info("sleeping(has data)... idle time:"+idletime+" ms");
//			Utils.sleep(idletime);
			logger.debug("received data, prepare to sleep for a while...");
			Util.sleepForawhile(3 * 1000);
			return true;
		} else{ //sleep while there is no data found
			logger.warn("no data found, sleeping...");
			Util.sleepForawhile(6 * 1000);
			return false;
		}
		
//		return mongoManager.getAvailableToRead(nextRead) > 0;
	}

	@Override
	public TransactionMetadata initializeTransaction(BigInteger txid,TransactionMetadata prevMetadata) {
//		logger.debug("in MSVTransactionalSpoutCoordinator - initializeTransaction...,txid:"+txid);
//		List<DBObject> batchRows = mongoManager.fetchAndUpdateAvailableToRead();
//		List<DBObject> batchRows = mongoManager.fetchAvailableToRead();
		TransactionMetadata ret = initializeTransactionMetadat();
		if(nextBatchRows!=null && !nextBatchRows.isEmpty()){
			List<ObjectId> rowIds = Util.convertToIdList(nextBatchRows);
//			quantity = quantity > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : quantity;
			ret = new TransactionMetadata(collectionName,rowIds);
			ObjectId[] objectIds = Util.convertObjectIdListToArr(rowIds);
			String objIdStr = Util.convertObjectIdArrToStr(objectIds);
			logger.debug("store transactionMetadata,txid:"+txid+" [collection:"+collectionName+"]objIds : "+objIdStr);
		}
//		nextRead += quantity;
//		mongoManager.updateFlagToInprogress(idList);
		return ret;
	}

	private TransactionMetadata initializeTransactionMetadat() {
		List<ObjectId> rowIds = new ArrayList<>();
		rowIds.add(new ObjectId("000000000000000000000001"));
		return new TransactionMetadata("PA",rowIds);
	}

	@Override
	public void close() {
		mongoManager.close();
	}


}
