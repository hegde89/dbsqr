package edu.unika.aifb.dbsqr.util;

import java.util.Map;
import java.util.TreeMap;

public class Timing {
	
	static class Stat {
		public int m_id;
		public String m_name;
		public long m_time;
		
		public Stat(int id) {
			this.m_id = id;
			this.m_time = 0;
		}
		
		public Stat(int id, String name) {
			this.m_id = id;
			this.m_name = name;
			this.m_time = 0;
		}
		
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Stat other = (Stat) obj;
			if(m_id != other.m_id)
				return false;
			if(m_name != other.m_name)
				return false;
			return true;
		} 
	}
	
	private Map<Integer,Stat> stats = new TreeMap<Integer,Stat>();
	
	private Log log;
	
	public Timing(String logFile) {
		this.log = new Log(logFile);
	}
	
	public void init(int maxId) {
		for(int id = 1; id <= maxId; id++) {
			addStat(id);
		}
	}
	
	public Stat addStat(int id) {
		Stat stat = new Stat(id);
		stats.put(id,stat);
		return stat;
	}
	
	public Stat addStat(int id, String name) {
		Stat stat = new Stat(id, name);
		stats.put(id, stat);
		return stat;
	}
	
	public void set(int id, long val) {
		stats.get(id).m_time = val;
	}
	
	public long get(int id) {
		return stats.get(id).m_time;
	}
	
	public void add(int id, long val) {
		stats.get(id).m_time += val;
	}
	
	public void reset() {
		for (Stat stat : stats.values()) {
			stat.m_time = 0;
		}
	}
	
	public void logStats() {
		log.open();
		log.writeLog("id : time spent");
		for(int id : stats.keySet()) {
			String message = id + " : " + stats.get(id).m_time;
			log.writeLog(message);
		}
		log.close();
	}
	
}
