package Experiments;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringBufferInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.tld.TLDManager;

public class DataExtractor {

	private static Logger log = Logger.getLogger(DataExtractor.class);
	
	private static int fromDsNum = 1;
	private static int endDsNum = 123;
	
	public static void main(String[] args) throws IOException, ParseException {
		long start = System.currentTimeMillis();
		
		TLDManager tldM = new TLDManager();

		PrintWriter pw = new PrintWriter("d://Test/data_test_btc/btc-2009-small_1-123.nq");
		BufferedReader reader = new BufferedReader(new FileReader("d://Test/data_test_btc/statistics/ds"));	
		List<String> dataSources = new ArrayList<String>();
		String line;
		int i = 1;
		int size = 0;
		while((line = reader.readLine()) != null && i <= endDsNum){
			if(i >= fromDsNum) {
				String[] str = line.split("\t");
				dataSources.add(str[0]);
				log.debug(line);
				size += Integer.valueOf(str[1]);
			}
			i++;
		}
		log.debug("Total number of statement: " + size);
		
		reader = new BufferedReader(new FileReader("d://Test/data_test_btc/btc-2009-small.nq"));	
		int triples = 0;
		size = 0;
		while((line = reader.readLine()) != null){
			NxParser nxp = new NxParser(new StringBufferInputStream(line));	
			while (nxp.hasNext()) {
				Node[] nodes = nxp.next();
				if (nodes.length > 3) {
					if (nodes[3] instanceof Resource) {
						String context = ((Resource) nodes[3]).toString();
						String ds = tldM.getPLD(new URL(context));
						if(dataSources.contains(ds)) {
							pw.println(line);
							size++;
						}	
					} 
					else
						log.error("context is not a resource");
				} 
				else
					log.error("there is no context");
			}
			triples++;
			if (triples % 1000000 == 0)
				log.debug("triples imported: " + triples);
		}
		log.debug("Total number of statement: " + size);
		
		pw.close();
		
		long end = System.currentTimeMillis();
		log.debug("time elapsed: " + (end - start) + "(ms)");
		
	}
}
