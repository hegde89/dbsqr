package edu.unika.aifb.ease.db;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Logger;

import edu.unika.aifb.ease.Environment;

public class DBService {
	
	private static final Logger log = Logger.getLogger(DBService.class); 
	
	private Connection m_conn;
	private String m_server;
	private String m_userName;
	private String m_password;
	private String m_port;
	private String m_dbName;
	
	/**
	 * Constructor
	 * @param server
	 * @param userName
	 * @param password
	 * @param port
	 * @param dbName
	 */
	public DBService(String server, String userName, String password, String port, String dbName, boolean createDB) {
		ResultSet rs = null;
		Statement stmt = null;
		try {
			this.m_server = server;
			this.m_userName = userName;
			this.m_password = password;
			this.m_port = port;
			this.m_dbName = dbName;
			if(createDB == true) {
				Class.forName("com.mysql.jdbc.Driver");
				m_conn = DriverManager.getConnection("jdbc:mysql://"+server+":"+port+"/", userName, password);
				stmt = m_conn.createStatement();
			
				DatabaseMetaData meta = m_conn.getMetaData();
				rs = meta.getCatalogs();
				Set<String> catalogs = new HashSet<String>();
				while(rs.next()) {
					catalogs.add(rs.getString(1));
				}
			
				log.info("---- Creating Database ----");
				if(catalogs.contains(m_dbName)) {
					stmt.executeUpdate("drop database " + m_dbName); 
				}
				String createDBSql = "create database " + m_dbName;
				stmt.execute(createDBSql);
			
				m_conn.setCatalog(m_dbName);
			}
			else {
				Class.forName("com.mysql.jdbc.Driver");
				m_conn = DriverManager.getConnection("jdbc:mysql://"+m_server+":"+m_port+"/"+m_dbName, m_userName, m_password);
			}
		} catch(SQLException e) {
			log.warn("Error while opening a conneciton to database server: " + e.getMessage());
		} catch(ClassNotFoundException e) {
			log.warn("Error while opening a conneciton to database server: " + e.getMessage());
		} finally {
			try {
				if(rs != null)
					rs.close();
				if(stmt != null)
					stmt.close();
			} catch(SQLException e) {
				log.warn("Error while closing the result set and the statement: " + e.getMessage());
			} 	
		}
	}
	
	/**
	 * Get an additional connection for nested queries
	 * @return
	 */
	public DBService getAdditionalConnection() {
		return new DBService(m_server, m_userName, m_password, m_port, m_dbName, false);
	}
	
	/**
	 * Close connection to the database server	 
	 */
	public void close(){
		try {
			m_conn.close();
		}
		catch (Exception e) {
			log.warn("Error while closing the connection to database server: " + e.getMessage());
		}
	}
	
	/**
	 * Execute a query, return a result set
	 * @param sqlstatement
	 * @return
	 */
	public ResultSet exectuteQuery(String sqlstatement) {
		try{
			Statement st = m_conn.createStatement();
			return st.executeQuery (sqlstatement);
		}
		catch(Exception e) {
			log.warn("Error while executing a query: " + e.getMessage());			
			return null;
		}
	}

	/**
	 * Create an SQL statement for executing later
	 * @return
	 */
	public Statement createStatement() {
		Statement stmt = null;
		try {
			stmt = m_conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(Integer.MIN_VALUE);
		} 
		catch (Exception e) {
			log.warn("Error while creating a statement: " + e.getMessage());
		}
		return stmt;
	}
	
	/**
	 * Create an SQL statement for executing later
	 * @param rsType
	 * @param rsConcurrency
	 * @return
	 */
	public Statement createStatement(int rsType, int rsConcurrency) {
		Statement stmt = null;
		try {
			stmt = m_conn.createStatement(rsType, rsConcurrency);
			stmt.setFetchSize(Integer.MIN_VALUE);
		} 
		catch (Exception e) {
			log.warn("Error while creating a statement: " + e.getMessage());
		}
		return stmt;    	
	}

	/**
	 * Create an SQL statement with parameters, which are updated later
	 * @param sql
	 * @return
	 */
	public PreparedStatement createPreparedStatement(String sql) {
		try {
			return m_conn.prepareStatement(sql);
		}
		catch (Exception e) {
			log.warn("Error while creating a prepare statement: " + e.getMessage());
		}
		return null;
	}

	/**
	 * Create an SQL statement with parameters, which are updated later
	 * @param sql
	 * @param rsType
	 * @param rsConcurrency
	 * @return
	 */
	public PreparedStatement createPreparedStatement(String sql, int rsType, int rsConcurrency) {
		try {
			return m_conn.prepareStatement(sql, rsType, rsConcurrency);
		}
		catch (Exception e) {
			log.warn("Error while creating a prepare statement: " + e.getMessage());
		}
		return null;
	}
	
	/**
	 * Get all tables in the database
	 * @return
	 */
	public Collection<String> getTables(){
		try {
			DatabaseMetaData metaData = m_conn.getMetaData();
			ResultSet rSet = metaData.getTables(null, null, null, null);
			Collection<String> tables = new HashSet<String>();
			while (rSet.next()) {
				String tableName = rSet.getString(3);
				tables.add(tableName);
			}
			rSet.close();
			return tables;
		}
		catch(Exception e){
			log.warn("Error while retrieving names of tables in the database: " + e.getMessage());
			return null;
		}
	}
	
	/**
	 * Check if a table exists in the database
	 * @param tableName
	 * @return
	 */
	public boolean hasTable(String tableName){
		Collection<String> tables = getTables();
    	return tables.contains(tableName.toLowerCase());
	}
	
	/**
	 * Retrieve file path for creating tempt files
	 * @param absPath
	 * @return
	 */
	public String getMySQLFilepath(String absPath){
		
		StringTokenizer fNameTokens = new StringTokenizer(absPath, File.separator);
    	String mysqlPath;

    	if(absPath.indexOf(":") > 0)
    		mysqlPath = "";    	
    	else
    		mysqlPath = "/";
    	
		while(fNameTokens.hasMoreTokens())
			mysqlPath += fNameTokens.nextToken() + "/";		
		mysqlPath = mysqlPath.substring(0, mysqlPath.length()-1);
		
		return mysqlPath;
	}

	/**
	 * Get connection
	 * @return
	 */
	public Connection getConnection(){
		return this.m_conn;
	}
	
	/**
	 * Get autoCommit
	 * @return
	 */
	public boolean getAutoCommit() {
		try {
			return m_conn.getAutoCommit();
		} catch (SQLException e) {
			log.warn("Error while setting autoCommit in the database: " + e.getMessage());
			return false;
		}
	} 
	
	/**
	 * Set autoCommit
	 * @param autoCommit
	 */
	public void setAutoCommit(boolean autoCommit) {
		try {
			m_conn.setAutoCommit(autoCommit);
		} catch (SQLException e) {
			log.warn("Error while setting autoCommit in the database: " + e.getMessage());
		}
	} 
}