package com.zliang.mystorm.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {
	
	private static final String configPath = "/app.properties";
	private static Properties prop;
	
	static{
		/*try {
			InputStream fis = Object.class.getClass().getResourceAsStream(configPath);
			prop = new Properties();
			prop.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		try {
			initialize();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void initialize() throws IOException{
		InputStream fis = Object.class.getClass().getResourceAsStream(configPath);
		prop = new Properties();
		prop.load(fis);
	}
	
	public static String getValue(String key){
		return prop.getProperty(key);
	}
	
	public static void main(String[] args) {
		String value = Configuration.getValue("mongodb.url2");
		System.out.println("value = "+(value==null?null:value));
	}

}
