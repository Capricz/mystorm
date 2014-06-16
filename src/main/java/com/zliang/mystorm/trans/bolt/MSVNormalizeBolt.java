package com.zliang.mystorm.trans.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import clojure.main;

import com.mongodb.DBObject;
import com.zliang.mystorm.util.Util;

public class MSVNormalizeBolt implements IBasicBolt {
	
	static Logger logger = Logger.getLogger(MSVNormalizeBolt.class);
	
//	MongoManager mongoManager;
	List<DBObject> batchRows;
	int batchSize = 2;
	int currIdx;
	
	public MSVNormalizeBolt(){
//		mongoManager = new MongoManager();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declareStream("normalizeBolt", new Fields(mongoManager.getCollectionFields()));
//		declarer.declare(new Fields("txid","rows"));
		declarer.declare(new Fields("rows"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		logger.debug("in MSVNormalizeBolt - getComponentConfiguration");
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		batchRows = new ArrayList<>();
		initializeCurrentState();
	}
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
//		TransactionAttempt tx = (TransactionAttempt) tuple.getValueByField("txid");
		DBObject row = (DBObject) tuple.getValueByField("row");
		logger.debug("in MSVNormalizeBolt - receive row : "+row);
		batchRows.add(row);
		currIdx++;
		logger.debug("in MSVNormalizeBolt - execute, currIdx : "+currIdx+", batchSize : "+batchSize);
		if(currIdx!=0 && currIdx % batchSize == 0){
			logger.debug("emit batch data, currIdx : "+currIdx+", batchSize : "+batchSize+", batchRows[size]:"+batchRows.size());
			collector.emit(new Values(batchRows));
//			initializeCurrentState();
		} else{
			logger.debug("accumulate tuple and skip the emit...");
			return;
		}
		Util.sleepForawhile(2 * 1000);
		/*String tweet = input.getStringByField("tweet");
		String tweetId = input.getStringByField("tweet_id");
		StringTokenizer strTok = new StringTokenizer(tweet, " ");
		TransactionAttempt tx = (TransactionAttempt)input.getValueByField("txid");
		HashSet<String> users = new HashSet<String>();
		while(strTok.hasMoreTokens()) {
			String user = strTok.nextToken();
			// Ensure this is an actual user, and that it's not repeated in the tweet
			if(user.startsWith("@") && !users.contains(user)) {
				collector.emit("users", new Values(tx, tweetId, user));
				users.add(user);
			}
		}*/
	}
	
	private void initializeCurrentState() {
		currIdx = 0;
		batchRows.clear();
		logger.debug("batchRows have been cleared");
	}

	@Override
	public void cleanup() {
//		mongoManager.close();
	}

}
