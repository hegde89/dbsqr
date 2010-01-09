package Experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.dbsqr.importer.TripleSink;

public class Partitioner {

	private static Logger log = Logger.getLogger(Partitioner.class);
	
	private static int numDs = 123;
	
	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		Statistics statistics = new Statistics();
		
		BufferedReader reader = new BufferedReader(new FileReader("d://Test/data_test_btc/statistics/ds"));	
		final Map<String,PrintWriter> dataSources = new HashMap<String,PrintWriter>();
		String line;
		int i = 1;
		while((line = reader.readLine()) != null && i <= numDs){
			String[] str = line.split("\t");
			File output = new File("d://Test/data_test_btc/dataSources/" + i + "_" + str[0] + "_" + str[1]); 
			if(!output.exists()) {
				output.getParentFile().mkdirs();
				output.createNewFile();
			}	
			final PrintWriter pw  = new PrintWriter(new FileWriter(output));
			dataSources.put(str[0],pw);
			log.debug(line);
			i++;
		}
		
		statistics.addImport("d://Test/data_test_btc/btc-2009-small.nq");
		statistics.setTripleSink(new TripleSink() {
			public void triple(String subject, String property, String object, String ds, int type){
				if(dataSources.keySet().contains(ds)) {
					dataSources.get(ds).println(subject + "\t" + property + "\t" + object);
				}	
			}
		});
		statistics.doImport();
		
		for(PrintWriter pw : dataSources.values()){
			pw.close();
		}
		
		long end = System.currentTimeMillis();
		log.debug("time elapsed: " + (end - start) + "(ms)");
		
	}

}
