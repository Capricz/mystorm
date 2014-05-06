package com.zliang.mystorm.spout;

import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoSpout implements IRichSpout {
	
	public static Logger log = LoggerFactory.getLogger(MongoSpout.class);
	
	final String host = "localhost";
	final int port = 27017;
	final String dataBase = "mydb";
	final String collection = "inst3";
	final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	
	long interval = 1000 * 2;
	
	SpoutOutputCollector _collector;
	Mongo mongoClient;
	DB db;
	DBCollection dbCollection;
	DBCursor cursor;
	DateFormat sdf;
	boolean _isEmitFinished;
	boolean _isDistributed;
	
	public MongoSpout(){
		this(true);
	}
	
	public MongoSpout(boolean isDistributed){
		this._isDistributed = isDistributed;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		log.debug("MongoSpout open...");
		this._collector = collector;
		
		try {
			mongoClient = new Mongo( host , port );
			db = mongoClient.getDB(dataBase);
			dbCollection = db.getCollection(collection);
			sdf = new SimpleDateFormat(DATE_FORMAT);
		} catch (UnknownHostException | NullPointerException e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}
	}

	@Override
	public void close() {
		log.debug("MongoSpout close...");
		cursor.close();
	}

	@Override
	public void activate() {
		log.debug("MongoSpout activate...");
	}

	@Override
	public void deactivate() {
		log.debug("MongoSpout deactivate...");
	}

	@Override
	public void nextTuple() {
		log.debug("MongoSpout nextTuple...");
		if(_isEmitFinished){
			Utils.sleep(interval);
		}
		if(dbCollection==null){
			log.error("dbCollection is empty...");
			return;
		}
		
		log.info("//////////////   query scope ts > now");
//		Timestamp now = new Timestamp(new Date().getTime());
		Date now = new Date();
		try {
			now = sdf.parse("2014-05-04 9:30:43.122");
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}
		BasicDBObject query = new BasicDBObject("d",new BasicDBObject("$gt",now));
		cursor = dbCollection.find(query);
		if(!cursor.hasNext()){
			log.info("no data found");
			return;
		}
		while(cursor.hasNext()){
			DBObject dbObject = (DBObject) cursor.next();
//			System.out.println(dbObject);
			Date d = (Date) dbObject.get("d");
			ObjectId objectId = (ObjectId) dbObject.get("_id");
			BSONTimestamp ts = (BSONTimestamp) dbObject.get("t");
			String user = (String) dbObject.get("user");
			String pass = (String) dbObject.get("pass");
			_collector.emit(new Values(objectId,ts,d,user,pass));
		}
		this._isEmitFinished = true;
	}

	@Override
	public void ack(Object msgId) {
		log.debug("MongoSpout ack...");
	}

	@Override
	public void fail(Object msgId) {
		log.debug("MongoSpout fail...(msgId:"+msgId+")");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("objectId","ts","d","user","pass"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
	}
	
}
