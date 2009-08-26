import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.tld.TLDManager;

public class NxImporter extends Importer {
	private static Logger log = Logger.getLogger(NxImporter.class);

	@Override
	public void doImport() {
		int triples = 0;
		try {
			for (String file : m_files) {
				log.debug("file: " + file);
				NxParser nxp = new NxParser(new FileInputStream(file));
				
				while (nxp.hasNext()) {
					Node[] nodes = nxp.next();
					
					String subject = null, property = null, object = null, context = null;
					
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
					}
					else if (nodes[2] instanceof BNode) {
						object = ((BNode)nodes[2]).toString();
					}
					else if (nodes[2] instanceof Literal) {
						object = ((Literal)nodes[2]).getData();
					}
					else 
						log.error("object is not a resource, bnode or literal");
					
					if (nodes.length > 3) {
						if (nodes[3] instanceof Resource) {
							context = ((Resource)nodes[3]).toString();
						}
						else
							log.error("context is not a resource");
					}
					
					m_sink.triple(subject, property, object, context);
					
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
		final TLDManager tldM = new TLDManager();
		tldM.readList("./res/tld.dat");
		
		NxImporter importer = new NxImporter();
		final HashSet<String> dataSources  = new HashSet<String>();
		importer.addImport("d://btc-2009-small.nq");
		importer.setTripleSink(new TripleSink() {
			public void triple(String subject, String property, String object, String context){
				try {
					dataSources.add(tldM.getPLD(new URL(context)));
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
			}
		});
		importer.doImport();
	
		File output = new File("d://ds");
		if(!output.exists())
			output.createNewFile();
		final PrintWriter pw  = new PrintWriter(new FileWriter(output));
		for(String ds : dataSources) {
			pw.println(ds);
		}
		
	}

}
