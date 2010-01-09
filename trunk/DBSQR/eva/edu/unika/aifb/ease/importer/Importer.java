package edu.unika.aifb.ease.importer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class Importer {
	
	protected TripleSink m_sink;
	protected List<String> m_files;

	protected Importer() {
		m_files = new ArrayList<String>();
	}
	
	public void addImport(String fileName) {
		m_files.add(fileName);
	}
	
	public void addImports(Collection<String> fileNames) {
		m_files.addAll(fileNames);
	}

	public void setTripleSink(TripleSink gb) {
		m_sink = gb;
	}

	public abstract void doImport();
}
