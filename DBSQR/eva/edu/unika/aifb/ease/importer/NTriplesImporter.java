package edu.unika.aifb.ease.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.ntriples.NTriplesParser;

public class NTriplesImporter extends Importer {
	private static Logger log = Logger.getLogger(NTriplesImporter.class);

	private boolean m_removeBN = true;
	
	public NTriplesImporter() {
		super();
	}
	
	@Override
	public void doImport() {
		TriplesHandler handler = new TriplesHandler(m_sink);

		for (String file : m_files) {
			handler.setDefaultContext(new File(file).getName());
					
			NTriplesParser parser = new NTriplesParser();
			parser.setDatatypeHandling(DatatypeHandling.VERIFY);
			parser.setStopAtFirstError(false);
			parser.setRDFHandler(handler);
			parser.setPreserveBNodeIDs(true);
			
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
