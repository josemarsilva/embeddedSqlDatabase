package org.josemarsilva.poc.embeddedSqlDatabase;

import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;


/**
 * Title:        EmbeddedSqlDatabase
 * Description:  An Embedded Sql Database abstraction to draft
 * Author:       Josemar Silva josemarsilva@inmetrics.com.br
 */
public class EmbeddedSqlDatabase {
	
	// SqlDatabase class properties
	
	Connection connection = null;
	static Logger logger = Logger.getLogger(EmbeddedSqlDatabase.class.getName());
	
	// SqlDatabase constants
	
	//
	// JDBC_DRIVER Samples:
	// 1. H2 - org.h2.Driver
	// 2. HSQL - org.hsqldb.jdbc.JDBCDriver
	// 3. DERBY – org.apache.derby.jdbc.ClientDriver or org.apache.derby.jdbc.EmbeddedDriver
	//
	//
	// DB_URL Samples:
	// 1. H2 – jdbc:h2:mem:dataSource
	// 2. HSQL – jdbc:hsqldb:mem:dataSource
	// 3. DERBY – jdbc:derby:memory:dataSource
	//
	private static String DB_URL = new String("jdbc:h2:mem:h2sqlmem");
	private static String DB_USER = new String("sa");
	private static String DB_PASSWORD = new String("");
	
	/**
	 * EmbeddedSqlDatabase constructor - Polymorphism #1.
	 * @throws SQLException 
	 */
	public void EmbeddedSqlDatabase(String url, String user, String pwd) throws SQLException {
		
		// Connect to database. This will load database files and start the 
		// database if its not already running.
		logger.info("getConnection(" + url + ", " + user + ", " + pwd +" )");
		connection = DriverManager.getConnection(url, user, pwd );
		
	}

	/**
	 * EmbeddedSqlDatabase constructor with all defaults parameters - Polymorphism #2.
	 */
	public EmbeddedSqlDatabase() throws SQLException {
		
		// Connect to database with default parameters
		EmbeddedSqlDatabase(DB_URL, DB_USER, DB_PASSWORD);
		
	}
	
	/**
	 * Execute SQL Statement - Polymorphism #1.
	 * @throws SQLException 
	 */
	public synchronized void execSqlStmt(String sqlStmt) throws SQLException {
		
		Statement st = null;
		logger.info("createStmt('"+ sqlStmt + "')");
		st = connection.createStatement();
		logger.info("executeStmt()");
		int i = st.executeUpdate(sqlStmt);
		if (i == -1) {
			logger.error("db error : " + sqlStmt);
        }
		logger.info("closeStmt()");
		st.close();
		
	}
	
	/**
	 * Execute SQL Statement - Polymorphism #2.
	 * @throws SQLException 
	 */
	public synchronized void execSqlStmt(String... sqlStmt) throws SQLException {
		for(int i=0; i<sqlStmt.length; i++) {
			logger.info("executeStmt( ["+i+"] )");
			execSqlStmt(sqlStmt[i]);
		}
	}

	
	/**
	 * Execute Query.
	 * @throws SQLException 
	 */
	public synchronized JsonObject execQuery(String sqlQuery) throws SQLException {
		
		// Create Statement SQL Query
		Statement stmt = null;
		logger.info("createStmt('"+ sqlQuery + "')");
		stmt = connection.createStatement();
		
		// Execute Query
		logger.info("executeQuery()");
		ResultSet resultSet = stmt.executeQuery(sqlQuery);
		
		// Create JSON Objects to store Java SQL Result Set
		logger.info("new JsonObject()");
		JsonObject jsonObjResultSet = new JsonObject();
		jsonObjResultSet.addProperty("title", "SQL Query Result Set");
		jsonObjResultSet.addProperty("sqlQuery", sqlQuery );
		JsonArray jsonObjRows = new JsonArray();
		
		// Get ResultSet MetaData
		logger.info("getMetaData()");
		ResultSetMetaData rsmd = resultSet.getMetaData();
        
		// Get ResultSet
		long rowNumber = 0;
        while(resultSet.next())
        {
        	// Next rowNumber
        	rowNumber++;
        	logger.info("results[" + rowNumber + "]");

        	// Create JSON Object for entire row
        	JsonObject jsonObjRow = new JsonObject();
        	jsonObjRow.addProperty("rowNumber", rowNumber);
        	
        	// Get ResultSet MetaData
            for (int i=1; i<=rsmd.getColumnCount(); i++)
            {
            	logger.info("row: " + rowNumber + "; colLable:" + rsmd.getColumnLabel(i).toLowerCase() + "; colType:" + rsmd.getColumnType (i));
        		switch (rsmd.getColumnType (i)) {
        		case  Types.BIGINT:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getLong(i));
        		case  Types.BINARY:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getShort(i));
        		case  Types.BIT:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getShort(i));
        		case  Types.INTEGER:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getInt(i));
        		case  Types.ROWID:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case  Types.SMALLINT:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getInt(i));
        		case  Types.TINYINT:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getInt(i));
        		case  Types.DECIMAL:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getDouble(i));
        		case  Types.DOUBLE:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getDouble(i));
        		case  Types.FLOAT:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getDouble(i));
        		case  Types.NUMERIC:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getDouble(i));
        		case  Types.DATE:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case  Types.DATALINK:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case  Types.TIMESTAMP:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case  Types.TIME:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case  Types.CHAR:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case Types.LONGNVARCHAR:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case Types.LONGVARCHAR:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case Types.NCHAR:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case Types.NULL:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case Types.NVARCHAR:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case Types.OTHER:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case Types.VARBINARY:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		case Types.VARCHAR:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		default:
                    jsonObjRow.addProperty(rsmd.getColumnLabel(i).toLowerCase(), resultSet.getString(i));
        		}
            }
            
            // Add to JSON Object this row from result set
            jsonObjRows.add(jsonObjRow);
            
        }
		
		// Close Statement
		logger.info("closeStmt()");
		stmt.close();
		
		// Return JSON Object
		jsonObjResultSet.add("resultset", jsonObjRows);
		return jsonObjResultSet;
		
	}
	

	/**
	 * Close connection to database.
	 * @throws SQLException 
	 */
	public void close() throws SQLException {
		
		// Close connection
		if (connection != null) {
			connection.close();
		}
		
	}


}
