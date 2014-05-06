package com.zliang.mystorm;

import com.zliang.mystorm.bolt.WordBolt;
import com.zliang.mystorm.spout.WordSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class MyWordTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word", new WordSpout(), 10);
		builder.setBolt("exclaim1", new WordBolt(),2).shuffleGrouping("word");
//		builder.setBolt("exclaim2", new WordBolt(),3).shuffleGrouping("exclaim1");
		
		Config config = new Config();
		config.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", config, builder.createTopology());
		
		Utils.sleep(1000*3);
		
		cluster.killTopology("test");
		
		cluster.shutdown();
	}

}
