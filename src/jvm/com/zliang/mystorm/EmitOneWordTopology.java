package com.zliang.mystorm;

import com.zliang.mystorm.bolt.EmitOneWordBolt;
import com.zliang.mystorm.bolt.WordBolt;
import com.zliang.mystorm.spout.EmitOneWordSpout;
import com.zliang.mystorm.spout.WordSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class EmitOneWordTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("emitOneWordSpout", new EmitOneWordSpout(), 10);
		builder.setBolt("emitOneWordBolt", new EmitOneWordBolt(),2).shuffleGrouping("emitOneWordSpout");
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
