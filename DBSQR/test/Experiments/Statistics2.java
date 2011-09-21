package Experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringBufferInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.tld.TLDManager;

import edu.unika.aifb.dbsqr.importer.TripleSink;

public class Statistics2 {

	private static Logger log = Logger.getLogger(Statistics2.class);

	private static int from = 500000;
	private static int end = 1000000;

	public static void main(String[] args) throws IOException, ParseException {
		long start = System.currentTimeMillis();
	
		BufferedReader reader = new BufferedReader(new FileReader("d://dbsqr/test329_pruned/ds_info"));	
		Set<String> dataSources = new HashSet<String>();
		String line;
		int size = 0;
		while((line = reader.readLine()) != null){
			String[] str = line.split("\t");
			if(str.length == 3) {
				int num = Integer.valueOf(str[2]);
				if(num < end && num >= from) {
					size += num;
					dataSources.add(str[1]);
				}	
			}
		}
		log.debug("Total number of statement: " + size);
		log.debug("Number of data sources: " + dataSources.size());
		log.debug("Average number of statement: " + size/dataSources.size());
	
		long end = System.currentTimeMillis();
		log.debug("time elapsed: " + (end - start) + "(ms)");
	}
}
