package com.zliang.mystorm.spout;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordSpout extends BaseRichSpout {
	public static Logger log = LoggerFactory.getLogger(WordSpout.class);
	SpoutOutputCollector _collector;
	boolean _isDistributed;

	public WordSpout() {
		this._isDistributed = false;
	}

	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		this._collector = collector;

	}

	@Override
	public void nextTuple() {
		log.info("########====> sleep1... 100ms");
		Utils.sleep(100);
		final String[] words = new String[] { "nathan", "mike", "jackson", "golda", "bertels" };
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		_collector.emit(new Values(word));
		log.info("########====> sleep2... 100ms");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if (!_isDistributed) {
			Map<String, Object> ret = new HashMap<String, Object>();
			ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
			return ret;
		} else {
			return null;
		}
	}

}
