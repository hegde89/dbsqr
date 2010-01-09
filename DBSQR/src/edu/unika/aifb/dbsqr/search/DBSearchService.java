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

public class DBSearchService {
	
private static final Logger log = Logger.getLogger(DBSearchService.class);
	
	private DBService m_dbService;
	private Config m_config;
	private PorterStemmer m_stemmer;
	
	public DBSearchService(Config config) {
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
			log.info("Time for Getting the Top-k Keyword Routing Plans: " + (end - start) + "(ms)");
			log.info("Time for Getting the Top-k Keyword Routing Plans: " + 
					(double) (end - start) / (double)1000  + "(sec)");
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
		Set<TermPair> termPairs = new HashSet<TermPair>();
		Map<Term, List<TermPair>> term2pair = new HashMap<Term,List<TermPair>>();
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
					term2pair.put(term, new ArrayList<TermPair>());
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
		
		int numTerms = terms.size();
		
		if(numTerms == 2) {
			TermPair termPair = new TermPair(terms.get(0),terms.get(1));
			return getSQLQuery2Keywords(termPair) + limitSql;
		}
		else if(numTerms > 2) {
			for(int i = 0; i < numTerms-1; i++) {
				for(int j = i+1; j <= numTerms-1; j++) {
					Term term1 = terms.get(i);
					Term term2 = terms.get(j);
					TermPair termPair = new TermPair(term1,term2);
					termPairs.add(termPair);
					term2pair.get(term1).add(termPair);
					term2pair.get(term2).add(termPair);
				}
			}
			return getSQLQueryMoreKeywords(terms, termPairs, term2pair) + limitSql;
		}
		else
			return null;
	}
	
	public String getSQLQuery2Keywords(TermPair termPair) {
		String selectSql = "select ";
		String fromSql = " from ";
		String whereSql = " where ";
		String groupSql = " group by ";
		String orderSql = " order by ";
		
		String tableAlias = getTableAlias(termPair);
		String datasourceColumn1 = getDatasourceColumn(termPair.getFirstTerm(), termPair);
		String datasourceColumn2 = getDatasourceColumn(termPair.getSecondTerm(), termPair);
		String keywordIdColumn1 = getKeywordIdColumn(termPair.getFirstTerm(), termPair);
		String keywordIdColumn2 = getKeywordIdColumn(termPair.getSecondTerm(), termPair);
		fromSql += Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + " as " + tableAlias;
		orderSql += tableAlias + "." + Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " DESC"; 
		selectSql += "'" + termPair.getFirstTerm().getText() + "'" + ", " + 
				tableAlias + "." + datasourceColumn1 + ", " + 
			"'" + termPair.getSecondTerm().getText() + "'" + ", " + 
				tableAlias + "." + datasourceColumn2;
		groupSql += tableAlias + "." + datasourceColumn1 + ", " + tableAlias + "." + datasourceColumn2;
		whereSql += tableAlias + "." + Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN + 
				" = " + termPair.getType() + 
			" and " + tableAlias + "." + keywordIdColumn1 + " = " + termPair.getFirstTerm().getId() + 
			" and " + tableAlias + "." + keywordIdColumn2 + " = " + termPair.getSecondTerm().getId();
		
		log.debug(selectSql);
		log.debug(fromSql);
		log.debug(whereSql);
		log.debug(groupSql);
		log.debug(orderSql);
		return selectSql + fromSql + whereSql + groupSql + orderSql;
	}
	
	public String getSQLQueryMoreKeywords(List<Term> terms, Set<TermPair> termPairs, 
			Map<Term, List<TermPair>> term2pair) {
		String selectSql = "select ";
		String fromSql = " from ";
		String whereSql = " where ";
		String groupSql = " group by ";
		String orderSql = " order by sum(";
		
		for(TermPair termPair : termPairs) {
			String tableAlias = getTableAlias(termPair);
			int type = termPair.getType(); 
			fromSql += Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + " as " + tableAlias + ", ";
			orderSql += tableAlias + "." + Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " + ";
			whereSql += tableAlias + "." + Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN + 
				" = " + type + " and ";
		}
		orderSql = orderSql.substring(0, orderSql.length() - 3) + ") DESC";
		fromSql = fromSql.substring(0, fromSql.length() - 2);
		
		for(Term term : term2pair.keySet()) {
			List<TermPair> list = term2pair.get(term);
			TermPair firstTermPair = list.get(0); 
			String tableAlias1 = getTableAlias(firstTermPair);
			String datasourceColumn = getDatasourceColumn(term,firstTermPair);
			String keywordIdColumn = getKeywordIdColumn(term,firstTermPair);
			
			selectSql += "'" + term.getText() + "'" + ", " + tableAlias1 + "." + datasourceColumn + ", ";
			groupSql += tableAlias1 + "." + datasourceColumn + ", ";
			
			whereSql += tableAlias1 + "." + keywordIdColumn + " = " + term.getId() + " and ";
			
			for(int i = 1; i < list.size(); i++) {
				TermPair termPair = list.get(i);
				String tableAlias2 = getTableAlias(termPair);
				whereSql += tableAlias2 + "." + getKeywordIdColumn(term,termPair) + 
								" = " + term.getId() + " and " + 
							tableAlias1 + "." + getConceptColumn(term,firstTermPair) + 
								" = " + tableAlias2 + "." + getConceptColumn(term,termPair) + " and ";
			}
		}
		selectSql = selectSql.substring(0, selectSql.length() - 2);
		groupSql = groupSql.substring(0, groupSql.length() - 2);
		whereSql = whereSql.substring(0, whereSql.length() - 5);
		
		log.debug(selectSql);
		log.debug(fromSql);
		log.debug(whereSql);
		log.debug(groupSql);
		log.debug(orderSql);
		return selectSql + fromSql + whereSql + groupSql + orderSql;
	}
	
	public String getTableAlias(TermPair termPair) {
		return termPair.getFirstTerm().getId() + "_" + 
			termPair.getSecondTerm().getId() + "_" + 
			termPair.getType();
	}
	
	public String getKeywordIdColumn(Term term, TermPair termPair) {
		if(term.equals(termPair.getFirstTerm()))
			return Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN;
		else if(term.equals(termPair.getSecondTerm()))
			return Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN;
		else 
			return null;
	} 
	
	public String getConceptColumn(Term term, TermPair termPair) {
		if(term.equals(termPair.getFirstTerm()))
			return Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN;
		else if(term.equals(termPair.getSecondTerm()))
			return Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN;
		else 
			return null;
	} 
	
	public String getDatasourceColumn(Term term, TermPair termPair) {
		if(term.equals(termPair.getFirstTerm()))
			return Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN;
		else if(term.equals(termPair.getSecondTerm()))
			return Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN;
		else 
			return null;
	} 
	
}
