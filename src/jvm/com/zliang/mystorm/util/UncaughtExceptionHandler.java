package com.zliang.mystorm.util;

import org.apache.log4j.Logger;


public class UncaughtExceptionHandler extends ThreadGroup {
	static Logger log = Logger.getLogger(UncaughtExceptionHandler.class);

	public UncaughtExceptionHandler(String name) {
		super(name);
	}

	/*public void uncaughtException(Thread t, Throwable e,MongoSpoutTask spoutTask) {
//		super.uncaughtException(t, e);
		if(!t.isInterrupted()){
			log.error("catch error : "+e.getMessage());
			t.interrupt();
			t = new Thread(spoutTask);
			t.start();
			log.info("restart thread");
		}
	}*/

}
