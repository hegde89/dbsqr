package edu.unika.aifb.dbsqr.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
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

public class NxImporter extends Importer {
	private static Logger log = Logger.getLogger(NxImporter.class);
	
	private TLDManager tldM;
	private Collection<String> dataSources;
	
	public NxImporter() {
		tldM = new TLDManager();
		try {
			tldM.readList("./res/tld.dat");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	
	public NxImporter(Collection<String> dataSources) {
		this.dataSources = dataSources;
		tldM = new TLDManager();
		try {
			tldM.readList("./res/tld.dat");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 

	@Override
	public void doImport() {
		int triples = 0;
		try {
			for (String file : m_files) {
				log.debug("file: " + file);
				NxParser nxp = new NxParser(new FileInputStream(file));
				
				while (nxp.hasNext()) {
					boolean toProcess = true;
					Node[] nodes = nxp.next();
					
					String subject = null, property = null, object = null, ds = null; 
					int type = Environment.UNPROCESSED;
					
					if (nodes[0] instanceof Resource) {
						subject = ((Resource)nodes[0]).toString();
						// to be removed later
						if(subject.length() > 200)
							toProcess = false;
					}
					else if (nodes[0] instanceof BNode) {
						subject = ((BNode)nodes[0]).toString();
						// to be removed later
						toProcess = false;
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
						// to be removed later
						if(object.length() > 200)
							toProcess = false;
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
						// to be removed later
						toProcess = false;
					}
					else if (nodes[2] instanceof Literal) {
						object = ((Literal)nodes[2]).getData();
						if(object.startsWith("http"))
							toProcess = false;
						// to be removed later
						if(object.length() > 50)
							toProcess = false;
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
					
					if(dataSources != null && dataSources.size() != 0) {
						if(dataSources.contains(ds)) {
							triples++;
							if(toProcess == true) {
								m_sink.triple(subject, property, object, ds, type);
							}
							else {
								m_sink.triple(subject, property, object, ds, Environment.UNPROCESSED);
							}
						}
					} 
					else {
						triples++;
						if(toProcess == true) {
							m_sink.triple(subject, property, object, ds, type);
						}
						else {
							m_sink.triple(subject, property, object, ds, Environment.UNPROCESSED);
						}
					}
					
					
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
		NxImporter importer = new NxImporter();
		final Map<String,Integer> dataSources  = new HashMap<String,Integer>();
		final Set<String> classes  = new HashSet<String>();
		importer.addImport("d://Test/btc_data/btc-2009-small.nq");
		importer.setTripleSink(new TripleSink() {
			public void triple(String subject, String property, String object, String ds, int type){
				int freq = (dataSources.get(ds) == null) ? 1 : (dataSources.get(ds)+1);
				dataSources.put(ds,freq);
				if(property.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					classes.add(object + "\t" + ds);
				}	
			}
		});
		importer.doImport();
	
		File dsOutput = new File("d://Test/btc_data/ds");
		if(!dsOutput.exists())
			dsOutput.createNewFile();
		PrintWriter pw  = new PrintWriter(new FileWriter(dsOutput));
		Map<String,Integer> dataSouces2 = sortByValue(dataSources); 
		for(String ds : dataSouces2.keySet()) {
			pw.println(ds + ": " + dataSouces2.get(ds));
		}
		pw.flush();
		pw.close();
		
		File classOutput = new File("d://Test/btc_data/class");
		if(!classOutput.exists())
			classOutput.createNewFile();
		pw  = new PrintWriter(new FileWriter(classOutput));
		for(String clazz : classes) {
			pw.println(clazz);
		}
		pw.flush();
		pw.close();
		
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
