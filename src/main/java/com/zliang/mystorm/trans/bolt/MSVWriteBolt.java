package com.zliang.mystorm.trans.bolt;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mongodb.DBObject;
import com.zliang.mystorm.trans.dao.MongoManager;
import com.zliang.mystorm.trans.dao.VerticaManager;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;

public class MSVWriteBolt extends BaseRichBolt {
	
	static Logger logger = Logger.getLogger(MSVWriteBolt.class);
	
	OutputCollector collector;
	MongoManager mongoManager;
	VerticaManager verticaManager;
//	List<DBObject> batchRows;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		logger.debug("in MSVWriteBolt - prepare");
		this.collector = collector;
		mongoManager = new MongoManager();
		verticaManager = new VerticaManager();
		logger.debug("in MSVCommiterBolt - instance batchRows...");
//		batchRows = new ArrayList<>();
	}

	@Override
	public void execute(Tuple tuple) {
		logger.debug("in MSVWriteBolt - execute");
//		List<Object> values = tuple.getValues();
//		TransactionAttempt tx = (TransactionAttempt) tuple.getValueByField("txid");
		List<DBObject> rows =  (List<DBObject>) tuple.getValueByField("rows");
		String collection = (String) tuple.getValueByField("collection");
		logger.debug("receive rows size :"+rows.size());
		if(rows!=null && !rows.isEmpty()){
			try {
				logger.debug("add row to vertica batch");
				for (DBObject row : rows) {
					verticaManager.addRowToBatch(row);
				}
				verticaManager.executeBatchRows();
				mongoManager.updateFlagToComplete(collection,rows);
			} catch (SQLException e) {
				e.printStackTrace();
				logger.debug(e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else{
			logger.debug("in MSVWriteBolt - receive rows is empty");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
