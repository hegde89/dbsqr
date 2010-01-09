package Experiments;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.tld.TLDManager;

import edu.unika.aifb.dbsqr.Environment;
import edu.unika.aifb.dbsqr.importer.NxImporter;
import edu.unika.aifb.dbsqr.importer.TripleSink;

public class Statistics {

	private static Logger log = Logger.getLogger(Statistics.class);
	
	private TLDManager tldM;
	private TripleSink m_sink;
	private List<String> m_files;
	
	public Statistics() {
		m_files = new ArrayList<String>();
		tldM = new TLDManager();
		try {
			tldM.readList("./res/tld.dat");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	public void doImport() {
		int triples = 0;
		try {
			for (String file : m_files) {
				log.debug("file: " + file);
				NxParser nxp = new NxParser(new FileInputStream(file));
				
				while (nxp.hasNext()) {
					Node[] nodes = nxp.next();
					
					String subject = null, property = null, object = null, ds = null; 
					int type = Environment.UNPROCESSED;
					
					if (nodes[0] instanceof Resource) {
						subject = ((Resource)nodes[0]).toString();
//						if(subject.length() > 100)
//							continue;
					}
					else if (nodes[0] instanceof BNode) {
						subject = ((BNode)nodes[0]).toString();
//						continue;
					}
					else 
						log.error("subject is neither a resource nor a bnode");
					
					if (nodes[1] instanceof Resource) {
						property = ((Resource)nodes[1]).toString();
					}
					else 
						log.error("property is not a resource");
					
					if (nodes[2] instanceof Resource) {
						object = ((Resource)nodes[2]).toString();
//						if(object.length() > 100)
//							continue;
						if(property.equals(RDF.TYPE.stringValue()) && 
								!(object.startsWith(RDF.NAMESPACE) || object.startsWith(RDFS.NAMESPACE) || 
										object.startsWith(OWL.NAMESPACE) || object.startsWith(XMLSchema.NAMESPACE))) {
							type = Environment.ENTITY_MEMBERSHIP_PROPERTY;
						}
						else if((property.startsWith(RDF.NAMESPACE) || property.startsWith(RDFS.NAMESPACE) || 
								property.startsWith(OWL.NAMESPACE) || property.startsWith(XMLSchema.NAMESPACE))) {
							type = Environment.RDFS_PROPERTY;
						}	
						else {
							type = Environment.OBJECT_PROPERTY;
						}
					}
					else if (nodes[2] instanceof BNode) {
						object = ((BNode)nodes[2]).toString();
						type = Environment.OBJECT_PROPERTY;
//						continue;
					}
					else if (nodes[2] instanceof Literal) {
						object = ((Literal)nodes[2]).getData();
//						if(object.length() > 50 || object.startsWith("http"))
//							continue;
						if((property.startsWith(RDF.NAMESPACE) || property.startsWith(RDFS.NAMESPACE) || 
								property.startsWith(OWL.NAMESPACE) || property.startsWith(XMLSchema.NAMESPACE))) {
							type = Environment.RDFS_PROPERTY;
						}
						else {
							type = Environment.DATA_PROPERTY;
						}
					}
					else 
						log.error("object is not a resource, bnode or literal");
					
					if (nodes.length > 3) {
						if (nodes[3] instanceof Resource) {
							String context = ((Resource) nodes[3]).toString();
							ds = tldM.getPLD(new URL(context));
						} 
						else
							log.error("context is not a resource");
					} 
					else
						ds = new File(file).getName();
					
					m_sink.triple(subject, property, object, ds, type);
					
					triples++;
					if (triples % 1000000 == 0)
						log.debug("triples imported: " + triples);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		Statistics statistics = new Statistics();
		
		File mappingOutput = new File("d://Test/data_test_btc/statistics/mappings"); 
		if(!mappingOutput.exists())
			mappingOutput.createNewFile();
		final PrintWriter mpw  = new PrintWriter(new FileWriter(mappingOutput));
		
		final Map<String,Integer> dataSources  = new HashMap<String,Integer>();
		final Map<String,Integer> classes  = new HashMap<String,Integer>();
		statistics.addImport("d://Test/data_test_btc/btc-2009-small.nq");
		statistics.setTripleSink(new TripleSink() {
			public void triple(String subject, String property, String object, String ds, int type){
				int dsfreq = (dataSources.get(ds) == null) ? 1 : (dataSources.get(ds)+1);
				dataSources.put(ds,dsfreq);
				if(property.equals(RDF.TYPE.stringValue())) {
					int classfreq = (classes.get(object + "\t" + ds) == null) ? 1 : (classes.get(object + "\t" + ds)+1);
					classes.put(object + "\t" + ds,classfreq);
				}	
				if(property.equals(RDFS.SEEALSO.stringValue()) || property.equals(OWL.SAMEAS.stringValue())) {
					mpw.println(subject + "\t" + object + "\t" + ds);
				}	
			}
		});
		statistics.doImport();
		
		mpw.close();

		File statOutput = new File("d://Test/data_test_btc/statistics/statistics"); 
		if(!statOutput.exists())
			statOutput.createNewFile();
		PrintWriter spw  = new PrintWriter(new FileWriter(statOutput));
		
		File dsOutput = new File("d://Test/data_test_btc/statistics/ds");
		if(!dsOutput.exists())
			dsOutput.createNewFile();
		PrintWriter pw  = new PrintWriter(new FileWriter(dsOutput));
		Map<String,Integer> dataSouces2 = sortByValue(dataSources); 
		int triples = 0;
		int numDs = 0;
		for(String ds : dataSouces2.keySet()) {
			int freq = dataSouces2.get(ds);
			triples += freq;
			numDs++;
			pw.println(ds + "\t" + freq);
			spw.println(numDs + "\t" + triples/numDs + "\t" + triples);
		}
		pw.println("Total number of tiples " + triples);
		pw.flush();
		pw.close();
		spw.println("Number of data sources : Average number of triples : Total number of triples");
		spw.close();
		
		File classOutput = new File("d://Test/data_test_btc/statistics/class");
		if(!classOutput.exists())
			classOutput.createNewFile();
		pw  = new PrintWriter(new FileWriter(classOutput));
		Map<String,Integer> classes2 = sortByValue(classes); 
		triples = 0;
		for(String clazz : classes2.keySet()) {
			int freq = classes2.get(clazz);
			triples += freq;
			pw.println(clazz + "\t" + freq);
		}
		pw.println("Total number of tiples " + triples);
		pw.flush();
		pw.close();
		
		long end = System.currentTimeMillis();
		log.debug("time elapsed: " + (end - start) + "(ms)");
	}
	
	public static <K,V extends Comparable<V>> Map<K,V> sortByValue(Map<K,V> map) {
	     List<Map.Entry<K,V>> list = new LinkedList<Map.Entry<K,V>>(map.entrySet());
	     Collections.sort(list, new Comparator<Map.Entry<K,V>>() {
	          public int compare(Map.Entry<K,V> o1, Map.Entry<K,V> o2) {
	               return o2.getValue().compareTo(o1.getValue());
	          }
	     });
	     Map<K,V> result = new LinkedHashMap<K,V>();
	     for (Iterator<Map.Entry<K, V>> it = list.iterator(); it.hasNext();) {
	    	 Map.Entry<K, V> entry = it.next();
	    	 result.put(entry.getKey(), entry.getValue());
	     }
	     return result;
	}

}
