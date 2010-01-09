package edu.unika.aifb.ease;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;


public class Config {
	
	private static final Logger log = Logger.getLogger(Config.class);

	//Server information
	private String m_server = Environment.DEFAULT_SERVER;
	private String m_username = Environment.DEFAULT_USERNAME;
	private String m_password = Environment.DEFAULT_PASSWORD;
	private String m_dbname = Environment.DEFAULT_DATABASE_NAME; 
	private String m_port = Environment.DEFAULT_PORT;

	//Data files
	private List<String> m_files;
  
	//Configuration information
	private int m_maxRadius = Environment.DEFAULT_MAX_RADIUS;
	private int m_topKeyword = Environment.DEFAULT_TOP_KEYWORD;
	private int m_topDatabase = Environment.DEFAULT_TOP_DATABASE;
	private int m_topResult = Environment.DEFAULT_TOP_RESULT;
	// For the r-radius graph whose size is too large, 
	// we consider it as a max r-radius graph without checking due to the efficiency issue. 
	// If the size of the r-Radius graph > 'm_sizeGraphWithoutCheck', 
	// we consider it as a as a max r-radius graph without checking.  
	private int m_sizeGraphWithoutCheck  = Environment.SIZE_R_RADIUS_GRAPH_WITHOUT_CHECK;
	
	//Temporary directory
	private String m_tempDir = Environment.DEFAULT_TEMPORAL_FILEPATH;
	private String m_stopwordFilePath = Environment.DEFAULT_STOPWORD_FILEPATH; 
	
	//singleton
	public static Config single = null;
	public static String configFilePath = null;

	public static void setConfigFilePath(String configFileName) {
		configFilePath = configFileName;
	}
	public static Config getConfig() {
		if(single == null) {
			single = new Config(configFilePath);
		}
		return single;
	}
	
	private Config(String configFileName) {
		load(configFileName);
	}
	
	public void load(String configFileName) {
		Map config = null;
		try {
			config = (Map)Yaml.load(new File(configFileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		m_server = config.get("server") != null ? (String)config.get("server") : Environment.DEFAULT_SERVER;
		m_username = config.get("username") != null ? (String)config.get("username") : Environment.DEFAULT_USERNAME;
		m_password = config.get("password") != null ? (String)config.get("password") : Environment.DEFAULT_PASSWORD;	
		m_dbname = config.get("dbname") != null ? (String)config.get("dbname") : Environment.DEFAULT_DATABASE_NAME;	
		m_port = config.get("port") != null ? ((Integer)config.get("port")).toString() : Environment.DEFAULT_PORT;
		m_maxRadius = config.get("maxRadius") != null ? (Integer)config.get("maxRadius") : Environment.DEFAULT_MAX_RADIUS;
		m_topKeyword = config.get("topKeyword") != null ? (Integer)config.get("topKeyword") : Environment.DEFAULT_TOP_KEYWORD;
		m_topDatabase = config.get("topDatabase") != null ? (Integer)config.get("topDatabase") : Environment.DEFAULT_TOP_DATABASE;	
		m_topResult = config.get("topResult") != null ? (Integer)config.get("topResult") : Environment.DEFAULT_TOP_RESULT;
		m_sizeGraphWithoutCheck = config.get("sizeWithoutCheck") != null ? (Integer)config.get("sizeWithoutCheck") : Environment.SIZE_R_RADIUS_GRAPH_WITHOUT_CHECK;
		m_tempDir = config.get("tempDirectory") != null ? (String)config.get("tempDirectory") : Environment.DEFAULT_TEMPORAL_FILEPATH;
		m_files = (List<String>)config.get("files");
		m_stopwordFilePath = config.get("stopword") != null ? (String)config.get("stopword") : Environment.DEFAULT_STOPWORD_FILEPATH;
	}
	
	public String getDbServer() {
		return m_server;
	}
	
	public String getDbUsername() {
		return m_username;
	} 
	
	public String getDbPassword() {
		return m_password;
	}
	
	public String getDbName() {
		return m_dbname;
	}
	
	public String getDbPort() {
		return m_port; 
	}
	
	public int getMaxRadius() {
		return m_maxRadius;
	}
	
	public int getTopKeyword() {
		return m_topKeyword;
	} 
	
	public int getTopDatabase() {
		return m_topDatabase;
	}
	
	public int getTopResult() {
		return m_topResult;
	}
	
	public int getSizeOfGraphWithoutCheck() {
		return m_sizeGraphWithoutCheck;
	}
	
	public List<String> getDataFiles() {
		return m_files;
	}
	
	public String getTemporaryDirectory() {
		return m_tempDir;
	}
	
	public String getStopwordFilePath() {
		return m_stopwordFilePath;
	}
	
}