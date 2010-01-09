package edu.unika.aifb.ease.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.tld.TLDManager;

import edu.unika.aifb.ease.Environment;

public class NxImporter extends Importer {
	private static Logger log = Logger.getLogger(NxImporter.class);
	
	private TLDManager tldM;
	
	public NxImporter() {
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
					Node[] nodes = nxp.next();
					
					String subject = null, property = null, object = null, ds = null; 
					int type = Environment.UNKNOWN;
					
					if (nodes[0] instanceof Resource) {
						subject = ((Resource)nodes[0]).toString();
					}
					else if (nodes[0] instanceof BNode) {
						subject = ((BNode)nodes[0]).toString();
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
						if((property.startsWith(RDF.NAMESPACE) || property.startsWith(RDFS.NAMESPACE)) && 
								(object.startsWith(RDF.NAMESPACE) || object.startsWith(RDFS.NAMESPACE))) {
							type = Environment.RDFS_PROPERTY;
						}	
						else if(property.equals(RDF.TYPE.stringValue())) {
							type = Environment.ENTITY_MEMBERSHIP_PROPERTY;
						}
						else {
							type = Environment.OBJECT_PROPERTY;
						}
					}
					else if (nodes[2] instanceof BNode) {
						object = ((BNode)nodes[2]).toString();
						type = Environment.OBJECT_PROPERTY;
					}
					else if (nodes[2] instanceof Literal) {
						object = ((Literal)nodes[2]).getData();
						if(object.length() > 100)
							continue;
						if((property.startsWith(RDF.NAMESPACE) || property.startsWith(RDFS.NAMESPACE))) {
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
		NxImporter importer = new NxImporter();
		final HashSet<String> dataSources  = new HashSet<String>();
		final HashSet<String> classes  = new HashSet<String>();
		importer.addImport("d://btc-2009-small.nq");
		importer.setTripleSink(new TripleSink() {
			public void triple(String subject, String property, String object, String ds, int type){
				dataSources.add(ds);
				if(property.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
					classes.add(object + "\t" + ds);
				}	
			}
		});
		importer.doImport();
	
		File dsOutput = new File("d://ds");
		if(!dsOutput.exists())
			dsOutput.createNewFile();
		PrintWriter pw  = new PrintWriter(new FileWriter(dsOutput));
		for(String ds : dataSources) {
			pw.println(ds);
		}
		pw.flush();
		pw.close();
		
		File classOutput = new File("d://class");
		if(!classOutput.exists())
			classOutput.createNewFile();
		pw  = new PrintWriter(new FileWriter(classOutput));
		for(String clazz : classes) {
			pw.println(clazz);
		}
		pw.flush();
		pw.close();
		
	}

}
