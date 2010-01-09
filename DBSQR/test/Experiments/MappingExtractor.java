package Experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.dbsqr.importer.TripleSink;

public class MappingExtractor {

	private static Logger log = Logger.getLogger(MappingExtractor.class);
	
	private static int numDs = 89;
	
	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		Statistics statistics = new Statistics();
		
		BufferedReader reader = new BufferedReader(new FileReader("d://Test/data_test_btc/statistics/ds"));	
		final Set<String> dataSources = new HashSet<String>();
		String line;
		int i = 1;
		while((line = reader.readLine()) != null && i <= numDs){
			i++;
			String[] str = line.split("\t");
			dataSources.add(str[0]);
			log.debug(line);
		}
		
		File mappingOutput = new File("d://Test/data_test_btc/statistics/mappings_89"); 
		if(!mappingOutput.exists())
			mappingOutput.createNewFile();
		final PrintWriter mpw  = new PrintWriter(new FileWriter(mappingOutput));
		
		statistics.addImport("d://Test/data_test_btc/btc-2009-small.nq");
		statistics.setTripleSink(new TripleSink() {
			public void triple(String subject, String property, String object, String ds, int type){
				if((property.equals(RDFS.SEEALSO.stringValue()) || property.equals(OWL.SAMEAS.stringValue()))
						&& dataSources.contains(ds)) {
					mpw.println(subject + "\t" + object + "\t" + ds);
				}	
			}
		});
		statistics.doImport();
		
		mpw.close();
		
		long end = System.currentTimeMillis();
		log.debug("time elapsed: " + (end - start) + "(ms)");
		
	}

}
