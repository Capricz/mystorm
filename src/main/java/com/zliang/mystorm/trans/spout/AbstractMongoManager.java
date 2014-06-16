package com.zliang.mystorm.trans.spout;

import java.io.Serializable;
import java.util.List;

public abstract class AbstractMongoManager implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final String NEXT_READ = "NEXT_READ";
	public static final String NEXT_WRITE = "NEXT_WRITE";
	
	public AbstractMongoManager(){
		
	}
	
//	public abstract boolean isAvailableToRead();
	
	public abstract long getNextRead();
	
	public abstract long getNextWrite();
	
	public abstract void close();
	
	public abstract void setNextRead(long nextRead);
	
//	public abstract List<String> getMessages(long from, int quantity);

}
