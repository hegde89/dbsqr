package edu.unika.aifb.ease.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.n3.N3ParserFactory;

public class N3Importer extends Importer {

	
	public N3Importer() {
		super();
	}
	
	@Override
	public void doImport() {
		TriplesHandler handler = new TriplesHandler(m_sink);

		for (String file : m_files) {
			handler.setDefaultContext(new File(file).getName());

			RDFParser parser = new N3ParserFactory().getParser();
			parser.setDatatypeHandling(DatatypeHandling.VERIFY);
			parser.setStopAtFirstError(false);
			parser.setRDFHandler(handler);
			parser.setVerifyData(false);

			try {
				parser.parse(
						new BufferedReader(new FileReader(file), 10000000), "");
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
