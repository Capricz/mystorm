package com.zliang.mystorm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MySQLBolt implements IRichBolt {
	
	public static Logger log = LoggerFactory.getLogger(MySQLBolt.class);
	
	OutputCollector _collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		StringBuffer sb = new StringBuffer();
		Integer userId = input.getInteger(0)+900000;
		String username = input.getString(1)+"#####";
		String password = input.getString(2)+"#####";
		String region = input.getString(3)+"#####";
		Integer roleId = input.getInteger(4)+900000;
		
		sb.append("userId:"+userId);
		sb.append(",username:"+username);
		sb.append(",password:"+password);
		sb.append(",region:"+region);
		sb.append(",roleId:"+roleId+"\n");
		
		log.info("[MySQLBolt]##===>"+sb.toString());
		
		List a = new ArrayList();
        a.add(input);
		_collector.emit(a,new Values(userId,username,password,region,roleId));
		_collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userId","username","password","region","roleId"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
