package edu.unika.aifb.ease.search;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.ease.Config;
import edu.unika.aifb.ease.Environment;
import edu.unika.aifb.ease.db.DBService;

public class DBSearchService {
	
private static final Logger log = Logger.getLogger(DBSearchService.class);
	
	private DBService m_dbService;
	private Config m_config;
	
	public DBSearchService(Config config) {
		m_config = config;
		initializeDbService();
		
	}
	
	public void initializeDbService() {
		String server = m_config.getDbServer();
		String username = m_config.getDbUsername();
		String password = m_config.getDbPassword();
		String port = m_config.getDbPort();
		String dbName = m_config.getDbName();
		m_dbService = new DBService(server, username, password, port, dbName, false);
	}
	
	public void close() {
		m_dbService.close();
	}
	
	/**
	 * Retrieve the centers of topK max r-Radius graphs that contain the given  keywords
	 * @param keywordList
	 * @param topK 
	 * @return the centers of top-k max r-Radius graphs that contain the given keywords, if k > 0; 
	 * 		   Otherwise, the centers of all max r-Radius graphs that contain the given  keywords
	 */
	public List<String> getMaxRRadiusGraphCenters(List<String> keywordList, int k) {
		long start = System.currentTimeMillis();
		
		List<String> centers = new ArrayList<String>();
		String sql = getQueryStatement(keywordList, k);
		Statement stmt = m_dbService.createStatement();
		try {
			ResultSet rs = stmt.executeQuery(sql);
			
			while(rs.next()) {
				String center = rs.getString(1);
				centers.add(center);
			} 

			rs.close();
			stmt.close();
			long end = System.currentTimeMillis();
			log.info("Time for Getting the Centers of Max r-Radius Graphs: " + (end - start) + "(ms)");
			log.info("Time for Getting the Centers of Max r-Radius Graphs: " + (double) (end - start) / (double)1000  + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of getting the centers of max r-Radius graphs from keyword graph inclusion table:");
			log.warn(ex.getMessage());
		} 
		
		return centers;
	} 
	
	public List<String> getMaxRRadiusGraphCenters(List<String> keywordList) {
		return getMaxRRadiusGraphCenters(keywordList, -1);
	} 
	
	public String getQueryStatement(List<String> keywordList, int k) {
		int numKeyword = keywordList.size(); 
		String selectSql = "select distinct " + "ET." + Environment.ENTITY_URI_COLUMN;
		String fromSql = " from ";
		String whereSql = " where ";
		String orderSql = " order by ";
		String limitSql = " limit " + k;
		
		for(int i = 0; i < numKeyword; i++) {
			fromSql += Environment.KEYWORD_TABLE + " as KT" + i + ", " + 
				Environment.KEYWORD_GRAPH_INCLUSION_TABLE +  " as KGT" + i + ", ";
			whereSql += "KT" + i + "." + Environment.KEYWORD_ID_COLUMN + " = " + "KGT" + i + "." + Environment.KEYWORD_GRAPH_INCLUSION_KEYWORD_ID_COLUMN + 
				" and " + "KT" + i + "." + Environment.KEYWORD_COLUMN + " = " + "'" + keywordList.get(i).toLowerCase() + "'" + " and ";
			orderSql += "KGT" + i + "." + Environment.KEYWORD_GRAPH_INCLUSION_SCORE_COLUMN + " + ";
		}
		fromSql = fromSql + Environment.ENTITY_TABLE + " as ET ";
		for(int j = 1; j < numKeyword; j++) {
			whereSql += "KGT" + 0 + "." + Environment.KEYWORD_GRAPH_INCLUSION_CENTER_ID_COLUMN + " = " +  
			"KGT" + j + "." + Environment.KEYWORD_GRAPH_INCLUSION_CENTER_ID_COLUMN + " and "; 
		}
		whereSql += "KGT" + 0 + "." + Environment.KEYWORD_GRAPH_INCLUSION_CENTER_ID_COLUMN + " = " + "ET." + Environment.ENTITY_ID_COLUMN;
		orderSql = orderSql.substring(0, orderSql.length() - 3);
		if(k > 0) {
			selectSql += fromSql + whereSql + orderSql + limitSql; 
		}
		else {
			selectSql += fromSql + whereSql; 
		}
		
		return selectSql;
	}

}
