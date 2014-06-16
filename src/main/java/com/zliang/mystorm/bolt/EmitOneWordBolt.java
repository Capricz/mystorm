package com.zliang.mystorm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EmitOneWordBolt extends BaseRichBolt {
	
	OutputCollector _collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String oneWord = input.getString(0);
		_collector.emit(input,new Values(oneWord+">>>"));
//		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("oneWord"));
	}

}
