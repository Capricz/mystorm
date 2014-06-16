package com.zliang.mystorm.trans.bolt;

import java.util.Map;

import com.zliang.mystorm.trans.spout.TransactionMetadata;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Tuple;

public class MSVWriteBatchBolt extends BaseBatchBolt<TransactionMetadata> {

	@Override
	public void prepare(Map conf, TopologyContext context,BatchOutputCollector collector, TransactionMetadata id) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple tuple) {
		/*
		 * String source = tuple.getSourceStreamId(); String tweetId =
		 * tuple.getStringByField("tweet_id"); if ("hashtags".equals(source)) {
		 * String hashtag = tuple.getStringByField("hashtag");
		 * add(tweetHashtags, tweetId, hashtag); } else if
		 * ("users".equals(source)) { String user =
		 * tuple.getStringByField("user"); add(userTweets, user, tweetId); }
		 */
	}

	@Override
	public void finishBatch() {
		/*for (String user : userTweets.keySet()) {
			Set<String> tweets = getUserTweets(user);
			HashMap<String, Integer> hashtagsCounter = new HashMap<String, Integer>();
			for (String tweet : tweets) {
				Set<String> hashtags = getTweetHashtags(tweet);
				if (hashtags != null) {
					for (String hashtag : hashtags) {
						Integer count = hashtagsCounter.get(hashtag);
						if (count == null)
							count = 0;
						count++;
						hashtagsCounter.put(hashtag, count);
					}
				}
			}
			for (String hashtag : hashtagsCounter.keySet()) {
				int count = hashtagsCounter.get(hashtag);
				collector.emit(new Values(id, user, hashtag, count));
			}
		}*/
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
