package com.zliang.mystorm.spout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zliang.mystorm.util.StormConstant;
import com.zliang.mystorm.util.StormUtils;

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
	String[] words;
	String field;

	public WordSpout() {
		field = StormUtils.getConfigProperty(StormConstant.MONGODB_PA_FIELDS);
		String[] fieldArr = field.split(",");
		if(fieldArr.length>0){
			field = fieldArr[0];
			log.info("read field in constructor : "+field);
		}
		this._isDistributed = false;
	}

	@Override
	public void open(Map config, TopologyContext context,SpoutOutputCollector collector) {
		this._collector = collector;
//		String[] words = (String[]) Utils.deserialize((byte[]) conf.get("words"));
//		log.info("############====>"+config.get("words"));
		List<String> wordsList = (List<String>) config.get("words");
		log.info("############ size ====>"+wordsList.size());
//		words = (String[]) wordsList.toArray();
		String[] tmpArr = new String[wordsList.size()];
		for (int i = 0 ; i < wordsList.size();i++) {
			tmpArr[i] = wordsList.get(i);
		}
		words = tmpArr;
//		field = (String) config.get("field");
		
	}

	@Override
	public void nextTuple() {
		log.info("########====> sleep1... 100ms");
		Utils.sleep(5000);
//		final String[] words = new String[] { "nathan", "mike", "jackson", "golda", "bertels" };
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		_collector.emit(new Values(word));
		log.info("########====> sleep2... 100ms");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(field));
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
