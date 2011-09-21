package edu.unika.aifb.dbsqr.search;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.dbsqr.Config;
import edu.unika.aifb.dbsqr.Environment;
import edu.unika.aifb.dbsqr.db.DBService;
import edu.unika.aifb.dbsqr.search.KeywordRoutingPlan.Keyword2DatasourceId;

public class DBSearchServiceByKeywordSet {
	
private static final Logger log = Logger.getLogger(DBSearchServiceByKeywordSet.class);
	
	private DBService m_dbService;
	private Config m_config;
	private PorterStemmer m_stemmer;
	
	public DBSearchServiceByKeywordSet(Config config) {
		m_config = config;
		m_stemmer = new PorterStemmer();
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
	 * Retrieve the topK keyword routing plans corresponding to the given keywords
	 * @param keywordList
	 * @param topK 
	 * @return the top-k keyword routing plans corresponding to the given keywords, if k > 0; 
	 * 		   Otherwise, the all keyword routing plans corresponding to the given keywords
	 */
	public List<KeywordRoutingPlan> computeKeywordRoutingPlans(List<String> keywordList, int k) {
		long start = System.currentTimeMillis();
		
		List<String> stemmedKeywordList = getStemmedKeywords(keywordList);
		List<KeywordRoutingPlan> plans = new ArrayList<KeywordRoutingPlan>();
		
		String sql = getQueryStatement(stemmedKeywordList, k);
		int numKeyword = stemmedKeywordList.size();
		Statement stmt = m_dbService.createStatement();
		try {
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next()) {
				KeywordRoutingPlan plan = new KeywordRoutingPlan();
				for(int i = 0; i <= 2*(numKeyword-1); i=i+2) {
					String keyword = rs.getString(i+1);
					int dsId = rs.getInt(i+2);
					plan.add(new Keyword2DatasourceId(keyword,dsId));
				}
				plans.add(plan);
			} 

			rs.close();
			stmt.close();
			long end = System.currentTimeMillis();
//			log.info("Time for Getting the Top-k Keyword Routing Plans: " + (end - start) + "(ms)");
//			log.info("Time for Getting the Top-k Keyword Routing Plans: " + 
//					(double) (end - start) / (double)1000  + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of getting the top-k keyword routing plans:");
			log.warn(ex.getMessage());
		} 
		
		return plans;
	} 
	
	public List<String> getStemmedKeywords(List<String> keywordList) {
		List<String> stemmedKeywordList = new ArrayList<String>(); 
		for(String keyword : keywordList) {
			stemmedKeywordList.add(m_stemmer.stem(keyword.toLowerCase()));
		}
		return stemmedKeywordList;
	} 
	
	public String getQueryStatement(List<String> keywordList, int k) {
		List<Term> terms = new ArrayList<Term>();
		String keywordQuerySql = "select " + Environment.COMPLETE_ID_COLUMN + ", " +
				Environment.COMPLETE_TYPE_COLUMN +
			" from " + Environment.COMPLETE_KEYWORD_TABLE + 
			" where " + Environment.COMPLETE_KEYWORD_COLUMN + " = ?"; 
		
		try {
			PreparedStatement ps = m_dbService.createPreparedStatement(keywordQuerySql);
			Iterator<String> iter = keywordList.iterator();
			while(iter.hasNext()) {
				String keyword = iter.next();
				ps.setString(1, keyword);
				ResultSet rs = ps.executeQuery();
				if(rs.next()) {
					Term term = new Term(keyword, rs.getInt(1), rs.getInt(2)); 
					terms.add(term);
				}
				else {
					iter.remove();
				}
				rs.close();
			}
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String limitSql = " limit " + k;
		
		return getSQLQuery(terms) + limitSql;
	}
	
	public String getSQLQuery(List<Term> terms) {
		String selectSql = "select ";
		String fromSql = " from ";
		String whereSql = " where ";
		String orderSql = " order by (";
		
		for(Term term : terms) {
			String tableAlias = getTableAlias(term);
			selectSql += "'" + term.getText() + "'" + ", " + 
				tableAlias + "." + Environment.KEYWORD_DS_INCLUSION_DS_ID_COLUMN + ", ";
			fromSql += Environment.KEYWORD_DS_INCLUSION_TABLE + " as " + tableAlias + ", ";
			whereSql += tableAlias + "." + Environment.KEXWORD_DS_INCLUSION_TYPE_COLUMN + " = " + term.getType() + 
				" and " + tableAlias + "." + Environment.KEYWORD_DS_INCLUSION_KEYWORD_ID_COLUMN + " = " + term.getId() + 
				" and ";
			orderSql += tableAlias + "." + Environment.KEYWORD_DS_INCLUSION_SCORE_COLUMN + " + ";
		}
		
		selectSql = selectSql.substring(0, selectSql.length() - 2);
		whereSql = whereSql.substring(0, whereSql.length() - 5);
		orderSql = orderSql.substring(0, orderSql.length() - 3) + ") DESC";
		fromSql = fromSql.substring(0, fromSql.length() - 2);
		
//		log.debug(selectSql);
//		log.debug(fromSql);
//		log.debug(whereSql);
//		log.debug(groupSql);
//		log.debug(orderSql);
		return selectSql + fromSql + whereSql + orderSql;
	} 
	
	public String getTableAlias(Term term) {
		return term.getId() + "_" + term.getType();
	}
	
}
