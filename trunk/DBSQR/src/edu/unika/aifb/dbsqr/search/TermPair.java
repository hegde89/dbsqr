package edu.unika.aifb.dbsqr.search;

import edu.unika.aifb.dbsqr.Environment;

public class TermPair {
	
	private Term m_term1;
	private Term m_term2;
	private int m_type; // 0: single-single; 1: single-compound; 2: compound-compound;
	
	public TermPair(Term keyword1, Term keyword2) {
		if(keyword1.compareTo(keyword2) < 0) {
			m_term1 = keyword1;
			m_term2 = keyword2;
		}
		else {
			m_term1 = keyword2;
			m_term2 = keyword1;
		}
		if(m_term1.getType() == Environment.TERM_SINGLE && 
				m_term2.getType() == Environment.TERM_SINGLE) {
			m_type = Environment.TERM_PAIR_SINGLE_SINGLE;
		}
		else if(m_term1.getType() == Environment.TERM_SINGLE && 
				m_term2.getType() == Environment.TERM_COMPOUND) {
			m_type = Environment.TERM_PAIR_SINGLE_COMPOUND;
		}
		else if(m_term1.getType() == Environment.TERM_COMPOUND && 
				m_term2.getType() == Environment.TERM_COMPOUND) {
			m_type = Environment.TERM_PAIR_COMPOUND_COMPOUND;
		}
	} 
	
	public Term getFirstTerm() {
		return m_term1;
	}  
	
	public Term getSecondTerm() {
		return m_term2;
	}  
	
	public int getType() {
		return m_type;
	}  
	
}
