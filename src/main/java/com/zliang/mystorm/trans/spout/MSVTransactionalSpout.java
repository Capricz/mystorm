package com.zliang.mystorm.trans.spout;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.tuple.Fields;

public class MSVTransactionalSpout extends BaseTransactionalSpout<TransactionMetadata> {
	
	static Logger logger = Logger.getLogger(MSVTransactionalSpout.class);
	
//	MongoManager mongoManager;
	
	public MSVTransactionalSpout(){
		logger.debug("in MSVTransactionalSpout - constructor");
//		mongoManager = new MongoManager();
	}

	@Override
	public backtype.storm.transactional.ITransactionalSpout.Coordinator<TransactionMetadata> getCoordinator(Map conf, TopologyContext context) {
		logger.debug("in MSVTransactionalSpout - initialize ITransactionalSpout.Coordinator");
		MSVTransactionalSpoutCoordinator coordinator = null;
		try {
			logger.debug("initializing MSVTransactionalSpoutCoordinator");
			coordinator = new MSVTransactionalSpoutCoordinator();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		if(coordinator==null){
			logger.error("coordinator is null");
			return null;
		}
		return coordinator;
	}

	@Override
	public backtype.storm.transactional.ITransactionalSpout.Emitter<TransactionMetadata> getEmitter(Map conf, TopologyContext context) {
		logger.debug("in MSVTransactionalSpout - initialize ITransactionalSpout.Emitter");
		return new MSVTransactionalSpoutEmitter();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		logger.debug("in MSVTransactionalSpout - initialize OutputFieldsDeclarer");
		/*String[] fields = mongoManager.getCollectionFields();
		String[] newArr = new String[fields.length+1];
		for (int i = 0; i < newArr.length; i++) {
			if(i==0){
				newArr[i] = "txid";
			} else{
				newArr[i] = fields[i-1];
			}
		}*/
		declarer.declare(new Fields("txid","collection","row"));
	}

}
