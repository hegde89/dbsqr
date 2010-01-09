package edu.unika.aifb.dbsqr.search;

public class Term implements Comparable<Term> {

	private String m_text;
	private int m_id;
	private int m_type; // 0: single; 1: compound; 
	
	public Term(String term, int id, int type) {
		this.m_text = term;
		this.m_type = type;
		this.m_id = id;
	} 
	
	public String getText() {
		return m_text;
	}  
	
	public int getId() {
		return m_id;
	}  
	
	public int getType() {
		return m_type;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + m_type;
		result = prime * result + m_id;
		result = prime * result + m_text.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Term other = (Term) obj;
		if(m_type != other.m_type)
			return false;
		if(m_id != other.m_id)
			return false;
		if(!m_text.equals(other.m_text))
			return false;
		return true;
	}

	@Override
	public int compareTo(Term keyword) {
		if(this.m_type > keyword.m_type)
			return 1;
		else if(this.m_type < keyword.m_type)
			return -1;
		else {
			if(this.m_id > keyword.m_id)
				return 1;
			else if(this.m_id < keyword.m_id)
				return -1;
			else 
				return 0;
		}
	}  
	
}
