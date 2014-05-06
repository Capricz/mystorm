package com.zliang.mystorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.zliang.mystorm.bolt.MySQLBolt;
import com.zliang.mystorm.spout.MySQLSpout;

public class MySQLTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("readSpout", new MySQLSpout(),5);
		builder.setBolt("readBolt", new MySQLBolt(),2).shuffleGrouping("readSpout");
		
		Config config = new Config();
		config.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("testrdbms", config, builder.createTopology());
		
		Utils.sleep(1000 * 30);
		//test kill
		cluster.killTopology("testrdbms");
		
		cluster.shutdown();
	}
}
