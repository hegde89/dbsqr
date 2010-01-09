package edu.unika.aifb.dbsqr.search;

import java.util.ArrayList;
import java.util.List;

public class KeywordRoutingPlan {

	public static class Keyword2DatasourceId {
		private String m_keyword;
		private int m_dsId;
		
		public Keyword2DatasourceId(String keyword, int dsId) {
			m_keyword = keyword;
			m_dsId = dsId;
		}
		
		public String getKeyword() {
			return m_keyword;
		}
		
		public int getDsId() {
			return m_dsId;
		}
	} 
	
	private List<Keyword2DatasourceId> m_list;
	
	public KeywordRoutingPlan() {
		m_list = new ArrayList<Keyword2DatasourceId>(); 
	}
	
	public void add(Keyword2DatasourceId keyw2id) {
		m_list.add(keyw2id);
	}
	
	public List<Keyword2DatasourceId> getKeywordRoutingPlan() {
		return m_list;
	}
}
