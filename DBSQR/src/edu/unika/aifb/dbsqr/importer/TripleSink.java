package edu.unika.aifb.dbsqr.importer;


public interface TripleSink {
	public void triple(String subject, String property, String object, int type, String datasource);
}
