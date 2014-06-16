package com.zliang.mystorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.zliang.mystorm.bolt.NormalizeDataBolt;
import com.zliang.mystorm.bolt.VerticaBolt;
import com.zliang.mystorm.spout.MongoSpout;

public class DataTransferTopology {
	public static void main(String[] args) {
		int parallelism_hint = 1;
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("readMongo", new MongoSpout(), parallelism_hint);
		builder.setBolt("normalizeData", new NormalizeDataBolt(), 1).shuffleGrouping("readMongo");
		builder.setBolt("writeVertica", new VerticaBolt(),1).fieldsGrouping("normalizeData", new Fields("user"));
		
		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(5);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("dataTransfer", config, builder.createTopology());
		
		Utils.sleep(1000 * 5);
		
//		cluster.killTopology("dataTransfer");
		
//		Utils.sleep(1000 * 5);
		
		cluster.shutdown();
	}
}
