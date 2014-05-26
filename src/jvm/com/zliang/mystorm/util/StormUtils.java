package com.zliang.mystorm.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import clojure.main;

public class StormUtils {
	
	public static final String PROP_FILE = "app.properties";
	
	public static Properties prop;
	
	static{
    	prop = new Properties();
    	InputStream in = StormUtils.class.getClassLoader().getResourceAsStream(PROP_FILE);
    	try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

	public static String[] convertListToArr(List<Object> columnsList) {
		String[] columnsArr = null;
		if(columnsList != null && !columnsList.isEmpty()){
			columnsArr = new String[columnsList.size()];
			for (int i=0;i<columnsList.size();i++) {
				columnsArr[i] = (String)columnsList.get(i);
			}
		}
		return columnsArr;
	}
	
	public static Properties getConfigProperties(){
		if(prop==null){
			initProperties();
		}
		return prop;
	}

	private static void initProperties() {
		prop = new Properties();
		InputStream in = StormUtils.class.getClassLoader().getResourceAsStream(PROP_FILE);
    	try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String getConfigProperty(String key){
		return StormUtils.getConfigProperties().getProperty(key);
	}
}
