package edu.unika.aifb.ease.importer;


public interface TripleSink {
	public void triple(String subject, String property, String object, String context, int type);
}
