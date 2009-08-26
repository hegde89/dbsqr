package edu.unika.aifb.dbsqr.importer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Importer {
	
	protected TripleSink m_sink;
	protected Map<String, List<String>> m_dsAndFilePaths;

	protected Importer() {
		m_dsAndFilePaths = new HashMap<String, List<String>>();
	}

	public void addImport(String ds, String file) {
		List<String> files = m_dsAndFilePaths.get(ds);
		if(files == null) {
			files = new ArrayList<String>();
			m_dsAndFilePaths.put(ds, files);
		}
		files.add(file);
	}

	public void setTripleSink(TripleSink gb) {
		m_sink = gb;
	}

	public abstract void doImport();
}
