package com.zliang.mystorm.bolt;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NormalizeDataBolt implements IRichBolt {
	
	public static Logger log = LoggerFactory.getLogger(NormalizeDataBolt.class);
	
	OutputCollector _collector;
	DateFormat sdf;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this._collector = collector;
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	}

	@Override
	public void execute(Tuple input) {
		ObjectId objectId = (ObjectId) input.getValue(0);
		List<Object> values = input.getValues();
		BSONTimestamp ts = (BSONTimestamp) values.get(1);
		Date d = (Date) values.get(2);
		String dateStr = sdf.format(d);
		String user = (String) values.get(3);
		String pass = (String)values.get(4);
		List a = new ArrayList();
		a.add(input);
		
//		_collector.emit(a, new Values(objectId,ts,dateStr,user,pass));
		_collector.emit(a, new Values(objectId,ts,d,user,pass));
		_collector.ack(input);
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("oid","ts","dateStr","user","pass"));
		declarer.declare(new Fields("oid","ts","d","user","pass"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
