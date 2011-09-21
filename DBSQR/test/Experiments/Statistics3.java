package Experiments;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.parser.ParseException;

public class Statistics3 {

	private static Logger log = Logger.getLogger(Statistics3.class);

	private static int from = 85;
	private static int end = 156;

	public static void main(String[] args) throws IOException, ParseException {
		long start = System.currentTimeMillis();
	
		BufferedReader reader = new BufferedReader(new FileReader("d://dbsqr/test156_pruned/distribution"));	
		int total = 0;
		String line;
		int size = 0;
		while((line = reader.readLine()) != null){
			String[] str = line.split("\t");
			if(str.length == 2) {
				int dsId = Integer.valueOf(str[0]);
				if(dsId <= end && dsId >= from) {
					total += Integer.valueOf(str[1]);
				}	
			}
		}
		log.debug("Total number of connections: " + (total+23712));
		log.debug("Number of data sources: " + (end - from + 1));
		log.debug("Average number of connections: " + total/(end - from + 1));
	
		long end = System.currentTimeMillis();
		log.debug("time elapsed: " + (end - start) + "(ms)");
	}

}
