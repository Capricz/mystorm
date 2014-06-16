package com.zliang.mystorm.trans.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;

import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.vertica.jdbc.VerticaConnection;
import com.zliang.mystorm.util.Const;
import com.zliang.mystorm.util.Util;

public class VerticaManager implements Serializable {
	
	private static final long serialVersionUID = 1L;

	static Logger logger = Logger.getLogger(VerticaManager.class);
	
	public static final String url = "jdbc:vertica://C0045453.itcs.hp.com:5433/Vertica";
	public static final String username = "dbadmin";
	public static final String password = "vertica123";
	public static final String filterFieldStr = Const.FIELD_TIME+","+Const.FIELD_FLAG;
	
	Connection conn;
    PreparedStatement pstmt;
    String[] fields = Util.getCollectionField();
    String[] keywords = Util.getKeywords();
//    String tableName = Const.VERTICA_TABLE_NAME;
    DateFormat sdf = new SimpleDateFormat(Const.DATE_FORMAT);

    String[] filterArr;
	
	public VerticaManager(){
		initialize();
	}

	public void initialize() {
		if(StringUtils.isNotBlank(filterFieldStr)){
			filterArr = filterFieldStr.split(",");
		}
		try {
			conn = DriverManager.getConnection(url, username, password);
//			((VerticaConnection) conn).setProperty("DirectBatchInsert", true);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	/*public void addRowToBatch(DBObject row) throws SQLException {
//		generatePreparedStatement(row);

		addToBatch(row);
	}*/

	public void addRowToBatch(DBObject row) throws SQLException {
		Set<String> keySet = row.keySet();
		int index = 0;
		for (String key : keySet) {
            logger.debug("the column from mongo : "+key);
            
//            if (fields != null && fields.length > 0) {

                
//                for (int i = 0; i < fields.length; i++) {
//                    index = i + 1;
//                    String field = fields[i];

                    //for filter '-', e.g. tracking-id -> trackingId
//			field = field.replaceAll("-", "");
//			Object object = input.getValue(i);
//                    Object object = input.getValueByField(field);
        	Object object = row.get(key);
        	String field = key.replaceAll("-|_", "");
        	if(Util.shouldFilterField(field,filterArr)){
        		continue;
        	}
        	index++;
                logger.debug("set pstmt field :" + field+ ", class :" + (object != null ? object.getClass() : null));
                if (object instanceof ObjectId) {
                    ObjectId objectId = (ObjectId) object;
                    pstmt.setString(index, objectId.toString());
                } else if (object instanceof Date) {
                    Date date = (Date) object;
					pstmt.setString(index, sdf .format(date));
                } else if (object instanceof Timestamp) {
                    Timestamp time = (Timestamp) object;
                    pstmt.setTimestamp(index, time);
                } else if (object instanceof BSONTimestamp) {
                    BSONTimestamp time = (BSONTimestamp) object;
                    pstmt.setTimestamp(index, new Timestamp(time.getTime()));
                } else if (object instanceof String) {
                    String value = (String) object;
                    logger.debug("String value : " + value);
                    pstmt.setString(index, value);
                } else if (object instanceof BasicDBObject) {
                    BasicDBObject dbObj = (BasicDBObject) object;
//					Set<Entry<String,Object>> entrySet = dbObj.entrySet();
                    pstmt.setString(index, dbObj.toString());
                } else if (null == object) {
                    pstmt.setString(index, "");
                } else {
                    String value = object.toString();
                    pstmt.setString(index, value);
                }
            }
            pstmt.addBatch();
    }

	public void generatePreparedStatement(String collectionName, DBObject row) throws SQLException,Exception {
		if(conn==null){
			initialize();
		}
		if(fields.length == filterArr.length){
			throw new Exception("error:column size equals to filter name list size");
		}
		logger.debug("Now inserting the rows to vertica!!!!!!!");

        String sql = " INSERT INTO " + collectionName + " (";
        StringBuffer sbColumn = new StringBuffer();
        StringBuffer sbValues = new StringBuffer();
        
        boolean isLastFilterField = false;
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
//                Object object = inputs.getValueByField(field);
            //for filter '-', e.g. tracking-id -> trackingId
            field = field.replaceAll("-|_", "");
            logger.debug("field name : "+field);
            //filter column by filter Array, check if the current one is last one
            if(i != fields.length - 1 && Util.shouldFilterField(field,filterArr)){
            	logger.debug("in the mid columns, i : "+i+", skip for filter...");
            	continue;
        	} else if(Util.shouldFilterField(field,filterArr)){
        		isLastFilterField = true;
        		logger.debug("last one is filter field, mark filterField is true, i : "+i);
        	} else{
        		logger.debug("normal field, name:"+field+", i:"+i);
        	}

            //add '\' for the field if it is a keyword in vertica
            if (Arrays.asList(keywords).contains(field)) {
                sbColumn.append("\"" + field + "\"" + ",");
            } else {
                sbColumn.append(field + ",");
            }

            //remove last comma,append sql for start of values
            if (i == fields.length - 1) {
                sbColumn.deleteCharAt(sbColumn.length() - 1);
                logger.debug("after remove last ',' sql:"+sbColumn.toString());
                //remove last one, since it is in filter name list
                if(isLastFilterField){
                	sbColumn.delete(sbColumn.lastIndexOf(","), sbColumn.length());
                	logger.debug("filter last column, sql:"+sbColumn.toString());
                }
                sbColumn.append(") VALUES(");
            }

            sbValues.append("?,");

            //remove last comma,append sql for end of values
            if (i == fields.length - 1) {
                sbValues.deleteCharAt(sbValues.length() - 1);
              //remove last one, since it is in filter name list
                if(isLastFilterField){
                	logger.debug("filter last '?', sql:"+sbValues.toString());
                	sbValues.delete(sbValues.lastIndexOf(","), sbValues.length());
                }
                sbValues.append(")");
            }
        }

        sql += sbColumn.toString() + sbValues;

        logger.debug("insert table: " + collectionName + ", sql : " + sql);

        //insert paramenter
		pstmt = conn.prepareStatement(sql);
	}

	public void executeBatchRows() throws Exception {
		logger.debug("pending execute inserting...");
		if(pstmt==null){
			throw new Exception("pstmt is null");
		}
        
		int[] count;
		try {
			count = pstmt.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
        logger.info("insert complete successfully, count : " + count);
	}
	
	public static void main(String[] args) {
		VerticaManager manager = new VerticaManager();
		
		DBObject row = new BasicDBObject();
		row.put("id", "123");
		row.put("group", "group1");
		row.put("name", "panasonic");
		row.put("time", new Timestamp((new Date()).getTime()));
		row.put("flag", 0);
		try {
			logger.debug("insert row : "+row);
			manager.addRowToBatch(row);
			manager.executeBatchRows();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void closePreparedStatment() {
		if(pstmt!=null){
			try {
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	}
}