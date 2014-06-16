package com.zliang.mystorm.trans.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.mongodb.DBObject;
import com.zliang.mystorm.trans.dao.MongoManager;
import com.zliang.mystorm.util.Util;


public class MSVReadSpout extends BaseRichSpout {
	
	static Logger logger = Logger.getLogger(MSVReadSpout.class);
	
	TransactionMetadata lastTransactionMetadata;
	MongoManager mongoManager;
	long nextRead = 0;
	long MAX_TRANSACTION_SIZE = 10;
	List<DBObject> nextBatchRows;
	
	SpoutOutputCollector collector;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		logger.debug("in MSVTransactionalSpoutCoordinator - prepare to initialize mongo");
		mongoManager = new MongoManager();
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
//		boolean ready = mongoManager.isAvailableToRead();
//		logger.info("ready to start a new transaction? : ["+(ready?"Yes":"No")+"]");
//		if(ready){
		try {
			List<DBObject> batchRows = null;//mongoManager.fetchAndUpdateAvailableToRead();
			if(batchRows!=null && !batchRows.isEmpty()){
				List<ObjectId> rowIds = Util.convertToIdList(batchRows);
//				quantity = quantity > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : quantity;
//				ret = new TransactionMetadata(rowIds);
				ObjectId[] objectIds = Util.convertObjectIdListToArr(rowIds);
				String objIdStr = Util.convertObjectIdArrToStr(objectIds);
				logger.debug("store transactionMetadata, objIds : "+objIdStr);
				logger.debug("fetch and update record flag from 0 to 1");
//			batchRows = mongoManager.fetchAndUpdateAvailableToRead(rowIds);
				logger.debug("get rows, size : "+batchRows.size());
				for (DBObject row : batchRows) {
					logger.debug("in MSVReadSpout - nextTuple - emit data,[row:"+row+"]");
//						collector.emit(new Values(tx,row));
					collector.emit(new Values(row));
				}
				long idletime = 1 * 1000;
				Util.sleepForawhile(idletime);
			}else{
				logger.info("no data found...");
				long idletime = 6 * 1000;
				Util.sleepForawhile(idletime);
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
//			mongoManager.updateFlagToDraft(batchRows);
		}
//		} else{ //sleep while there is no data found
//			long idletime = 6 * 1000;
//			logger.info("sleeping(no data)... idle time:"+idletime+" ms");
//			Utils.sleep(idletime);
//			return;
//		}
		
//		logger.debug("in initializeTransaction...,txid:"+txid);
//		List<DBObject> batchRows = mongoManager.fetchAndUpdateAvailableToRead();
		
//		TransactionMetadata ret = initializeTransactionMetadat();
		
		
		
	}
	
	private TransactionMetadata initializeTransactionMetadat() {
		List<ObjectId> rowIds = new ArrayList<>();
		rowIds.add(new ObjectId("000000000000000000000001"));
		return new TransactionMetadata("PA",rowIds);
	}

	@Override
	public void close() {
		mongoManager.close();
		super.close();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		logger.debug("in MSVReadSpout - initialize OutputFieldsDeclarer");
		declarer.declare(new Fields("row"));
	}


}
