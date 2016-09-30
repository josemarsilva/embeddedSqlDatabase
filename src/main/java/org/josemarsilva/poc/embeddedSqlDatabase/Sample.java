package org.josemarsilva.poc.embeddedSqlDatabase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import org.josemarsilva.poc.embeddedSqlDatabase.EmbeddedSqlDatabase;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

/**
 * Title:        EmbeddedSqlDatabase - Main Demo
 * Description:  EmbeddedSqlDatabase Main Demonstration
 * Author:       Josemar Silva josemarsilva@inmetrics.com.br
 */
public class Sample 
{

	/**
	 * main() contains some samples EmbeddedSqlDatabase
	 * @param args
	 * @throws SQLException
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
    public static void main( String[] args ) throws SQLException, FileNotFoundException, IOException
    {
    	// Configure log4
        org.apache.log4j.BasicConfigurator.configure();
        
        // Call sample1() - New EmbeddedSqlDatabase, Deploy some create tables and views, Query some data
//        sample1();
        
        // Call sample2() - New multiples connections to database (>1), create the same table name with the same name but in different schemas
//        sample2();
        
        // Call sample3() - Import txt file into table
        sample3();
                
    }

    /**
     * sample1(): New EmbeddedSqlDatabase, Deploy some create tables and views, Query some data
     * @throws SQLException 
     */
    public static void sample1() throws SQLException {
    	
        // New Embedded Sql Database instance
        EmbeddedSqlDatabase embeddedSqlDatabase = new EmbeddedSqlDatabase();
        
        // New Gson JSON Builder to print JSON Objects
        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();

        
        // Deploy some tables objects with some data - See http://www.h2database.com/html/grammar.html
        embeddedSqlDatabase.execSqlStmt("CREATE TABLE tabSample1 ( id INTEGER IDENTITY PRIMARY KEY, str_col VARCHAR(256), num_col INTEGER, dt_col DATE, db_col DOUBLE)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('Legiao Urbana',           1, '1980-01-01', 1.01)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('Os Paralamas do Sucesso', 2, '1980-01-01', 22.02)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('Lobão',                   3, '1980-01-01', 333.03)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('Barão Vermelho',          4, '1980-01-01', 4444.04)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('Titãs',                   5, '1980-01-01', 55555.05)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('RPM',                     6, '1980-01-01', 666666.06)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('Rita Lee',                7, '1980-01-01', 7777777.07)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('Capital Inicial',         8, '1980-01-01', 88888888.08)");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample1 (str_col, num_col, dt_col, db_col) VALUES('Ultraje a Rigor',         9, '1980-01-01', 999999999.09)");
        embeddedSqlDatabase.execSqlStmt("CREATE INDEX idx_num_col ON tabSample1 (num_col)");
        embeddedSqlDatabase.execSqlStmt("CREATE TABLE tabSample2 ( id INTEGER IDENTITY PRIMARY KEY, num_col INTEGER, str_col VARCHAR(2000) )");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample2 (num_col, str_col) VALUES(1, '                                                                                                   1         1         1         1         1         1         1         1         1         1         2         2         2         2         2         2         2         2         2         2         3')");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample2 (num_col, str_col) VALUES(2, '         1         2         3         4         5         6         7         8         9         0         1         2         3         4         5         6         7         8         9         0         1         2         3         4         5         6         7         8         9         0')");
        embeddedSqlDatabase.execSqlStmt("INSERT INTO tabSample2 (num_col, str_col) VALUES(3, '123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890')");
        embeddedSqlDatabase.execSqlStmt("CREATE VIEW vwSample2 AS SELECT num_col, SUBSTRING(str_col,1,10) str_01_a_10, SUBSTRING(str_col,170,30) str_170_a_200 FROM tabSample2");
        embeddedSqlDatabase.execSqlStmt(
        		"CREATE TABLE tabSample3 ( id INTEGER IDENTITY PRIMARY KEY, str_col VARCHAR(256), num_col INTEGER, dt_col DATE, db_col DOUBLE)", 
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('Led Zeppelin',            1, '1980-01-01', 1.01)",
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('The Beatles',             2, '1980-01-01', 22.02)",
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('Pink Floyd ',             3, '1980-01-01', 333.03)",
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('Queen',                   4, '1980-01-01', 4444.04)",
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('Van Halen',               5, '1980-01-01', 55555.05)",
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('The Rolling Stones',      6, '1980-01-01', 666666.06)",
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('AC/DC',                   7, '1980-01-01', 7777777.07)",
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('Guns N Roses',            8, '1980-01-01', 88888888.08)",
                "INSERT INTO tabSample3 (str_col, num_col, dt_col, db_col) VALUES('The Beatles',             9, '1980-01-01', 999999999.09)"
                );
        
        // Query some data        
        JsonObject jsonObjQuery1 = new JsonObject();
        JsonObject jsonObjQuery2 = new JsonObject();
        JsonObject jsonObjQuery3 = new JsonObject();
        JsonObject jsonObjQuery4 = new JsonObject();
        jsonObjQuery1 = embeddedSqlDatabase.execQuery("SELECT * FROM tabSample1");
        jsonObjQuery2 = embeddedSqlDatabase.execQuery("SELECT * FROM vwSample2");
        jsonObjQuery3 = embeddedSqlDatabase.execQuery("SELECT * FROM tabSample3");
        jsonObjQuery4 = embeddedSqlDatabase.execQuery(
        		"SELECT tab1.id      AS RockRanking," +
        		"       tab1.str_col AS Nacional, " +
        		"       tab3.str_col AS Internacional " +
        		"FROM   tabSample1 tab1 " +
        		"INNER  JOIN tabSample3 tab3 " +
        		"ON     tab1.id = tab3.id " +
        		"ORDER  by tab1.id");
        
        // Printing JSON
        System.out.println(gson.toJson(jsonObjQuery1));
        System.out.println(gson.toJson(jsonObjQuery2));
        System.out.println(gson.toJson(jsonObjQuery3));
        System.out.println(gson.toJson(jsonObjQuery4));
        
        // Close Embedded SQL Database connection                
        embeddedSqlDatabase.close();
    	
    }
    
    /**
     * sample2(): New multiples connections to database (>1), create the same table name with the same name but in different schemas
     * @throws SQLException 
     */
    public static void sample2() throws SQLException {
    	
        // New multiples embeddedSqlDatabase on the same thread
        EmbeddedSqlDatabase embeddedSqlDatabase2 = new EmbeddedSqlDatabase();
        EmbeddedSqlDatabase embeddedSqlDatabase3 = new EmbeddedSqlDatabase();
        
        // New Gson JSON Builder to print JSON Objects
        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();

        // Create the same table "tabSample3" in another schema "schemaCt2" with different data
        embeddedSqlDatabase2.execSqlStmt(
        		"CREATE SCHEMA schemaCt2",
        		"CREATE TABLE schemaCt2.tabSample3 ( id INTEGER IDENTITY PRIMARY KEY, str_col VARCHAR(256), num_col INTEGER, dt_col DATE, db_col DOUBLE)", 
                "INSERT INTO schemaCt2.tabSample3 (str_col, num_col, dt_col, db_col) VALUES('José',  1, '1980-01-01', 1.01)",
                "INSERT INTO schemaCt2.tabSample3 (str_col, num_col, dt_col, db_col) VALUES('João',  2, '1980-01-01', 22.02)"
                );

        // Query some data        
        JsonObject jsonObjQuery5 = new JsonObject();
        jsonObjQuery5 = embeddedSqlDatabase2.execQuery("SELECT * FROM schemaCt2.tabSample3");
        
        // Printing JSON
        System.out.println(gson.toJson(jsonObjQuery5));
        
        // Now, trying to create the same table "tabSample3" in another schema "schemaCt3" with different data
        embeddedSqlDatabase3.execSqlStmt(
        		"CREATE SCHEMA schemaCt3",
        		"CREATE TABLE schemaCt3.tabSample3 ( id INTEGER IDENTITY PRIMARY KEY, str_col VARCHAR(256), num_col INTEGER, dt_col DATE, db_col DOUBLE)", 
                "INSERT INTO schemaCt3.tabSample3 (str_col, num_col, dt_col, db_col) VALUES('Maria', 1, '1980-01-01', 1.01)",
                "INSERT INTO schemaCt3.tabSample3 (str_col, num_col, dt_col, db_col) VALUES('Ana',   2, '1980-01-01', 22.02)"
                );
        
        // Query some data        
        JsonObject jsonObjQuery6 = new JsonObject();
        jsonObjQuery5 = embeddedSqlDatabase3.execQuery("SELECT * FROM schemaCt3.tabSample3");
        
        // Printing JSON
        System.out.println(gson.toJson(jsonObjQuery6));
        
        // Close Embedded SQL Database connection                
        embeddedSqlDatabase2.close();
        embeddedSqlDatabase3.close();

    }

    public static void sample3() throws SQLException, FileNotFoundException, IOException {
    	
        // New Embedded Sql Database instance
        EmbeddedSqlDatabase embeddedSqlDatabase = new EmbeddedSqlDatabase();
        
        // Load a file
        embeddedSqlDatabase.loadFile("log4j.properties", "schemaCt3.loadtable", EmbeddedSqlDatabase.LOAD_FILE_TYPE_TXT);

        // Create view to parse table
        embeddedSqlDatabase.execSqlStmt("CREATE VIEW schemaCt3.vwSample AS SELECT num_row, SUBSTRING(str_txt,1,200) AS substr FROM schemaCt3.loadtable WHERE LENGTH(str_txt) > 0 AND INSTR(str_txt,'#') > 0 ");

        // Export table to a file
        embeddedSqlDatabase.exportQuery("sample3-table-json.txt", "SELECT * FROM schemaCt3.loadtable ORDER BY num_row", EmbeddedSqlDatabase.EXPORT_FILE_TYPE_JSON);

        // Export view to a file
        embeddedSqlDatabase.exportQuery("sample3-view-json.txt", "SELECT * FROM schemaCt3.vwSample" );

        
    }
    
    
}
