package edu.unika.aifb.dbsqr.importer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.rdfxml.RDFXMLParser;

public class RDFImporter extends Importer {
	private static Logger log = Logger.getLogger(RDFImporter.class);

	public RDFImporter() {
		super();
		log = Logger.getLogger(RDFImporter.class);
	}
	
	@Override
	public void doImport() {
		for (String ds : m_dsAndFilePaths.keySet()) {
			TriplesHandler handler = new TriplesHandler(m_sink);
			log.info("Indexing data source " + ds);
			handler.setDatasource(ds);
			for (String file : m_dsAndFilePaths.get(ds)) {
				RDFXMLParser parser = new RDFXMLParser();
				parser.setDatatypeHandling(DatatypeHandling.VERIFY);
				parser.setStopAtFirstError(false);
				parser.setRDFHandler(handler);
				parser.setVerifyData(false);

				try {
					parser.parse(new BufferedReader(new FileReader(file), 10000000), "");
				} catch (RDFParseException e) {
					e.printStackTrace();
				} catch (RDFHandlerException e) {
					e.printStackTrace();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
