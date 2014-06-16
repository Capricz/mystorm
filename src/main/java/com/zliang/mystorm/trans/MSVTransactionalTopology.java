package com.zliang.mystorm.trans;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.utils.Utils;

import com.zliang.mystorm.trans.bolt.MSVCommiterBatchBolt;
import com.zliang.mystorm.trans.bolt.MSVNormalizeBatchBolt;
import com.zliang.mystorm.trans.spout.MSVReadSpout;
import com.zliang.mystorm.trans.spout.MSVTransactionalSpout;

public class MSVTransactionalTopology {
	
	static Logger logger = Logger.getLogger(MSVTransactionalTopology.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		logger.info("start logging...");
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("test", "spout", new MSVTransactionalSpout());
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("spout", new MSVReadSpout());
		builder.setBolt("normaize-data", new MSVNormalizeBatchBolt(), 4).shuffleGrouping("spout");
		builder.setBolt("vertica-committer", new MSVCommiterBatchBolt()).shuffleGrouping("normaize-data");
//		builder.setBolt("vertica-write", new MSVWriteBolt()).shuffleGrouping("normaize-data");
		
		Config config = new Config();
		config.setDebug(true);
		logger.info("create config...");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", config, builder.buildTopology());
		
		long idleTime = 60 * 60 * 1000;
		logger.info("prepare to sleeping... "+idleTime+"ms");
		Utils.sleep(idleTime);
		
		
		logger.info("prepare to shutdown...");
		cluster.shutdown();
		/*int parallelism_hint = 1;
		
		
		
		builder.setBolt("write-vertica",new MSVWriteBatchBolt(), 4).shuffleGrouping("normaize-data");
		
		
		
		config.setNumWorkers(5);
		
		
		
		
		
//		cluster.killTopology("dataTransfer");
		
//		Utils.sleep(1000 * 5);
		
		*/
	}

}
