package edu.unika.aifb.ease.importer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.semanticweb.yars.tld.TLDManager;

import edu.unika.aifb.ease.Environment;

class TriplesHandler implements RDFHandler {
	private static Logger log = Logger.getLogger(TriplesHandler.class);

	private int m_triplesTotal;
	private int m_triplesAdded;
	private String m_defaultContext;
	private TripleSink m_sink;
	private TLDManager tldM;
	
	public TriplesHandler(TripleSink sink) {
		m_sink = sink;
		tldM = new TLDManager();
		try {
			tldM.readList("./res/tld.dat");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setDefaultContext(String context) {
		m_defaultContext = context;
	}
	
	public void startRDF() throws RDFHandlerException {
	}
	
	public void endRDF() throws RDFHandlerException {
	}

	public void handleComment(String arg0) throws RDFHandlerException {
	}

	public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
	}

	public void handleStatement(Statement st) throws RDFHandlerException {
		m_triplesTotal++;
		int type = Environment.UNKNOWN; 
		
		String subject = null; 
		if (st.getSubject() instanceof org.openrdf.model.URI) {
			subject = ((org.openrdf.model.URI)st.getSubject()).toString();
		}
		else if (st.getSubject() instanceof BNode) {
			BNode bn = (BNode)st.getSubject();
			subject = bn.getID();
			if (!subject.startsWith("http"))
				subject = "_:" + subject;
		}
		else {
			log.warn("subject is not an URI or a blank node, ignoring " + st.getSubject().getClass());
			return;
		}
		
		String property = st.getPredicate().toString();
		
		String object = null;
		if (st.getObject() instanceof org.openrdf.model.URI) {
			object = ((org.openrdf.model.URI)st.getObject()).toString();
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
		else if (st.getObject() instanceof Literal) {
			Literal lit = (Literal)st.getObject();
			object = lit.stringValue();
			object = object.replaceAll("\n", "\\\\" + "n");
			if(object.length() > 100)
				return;
			if((property.startsWith(RDF.NAMESPACE) || property.startsWith(RDFS.NAMESPACE))) {
				type = Environment.RDFS_PROPERTY;
			}
			else {
				type = Environment.DATA_PROPERTY;
			}
		}
		else if (st.getObject() instanceof BNode) {
			BNode bn = (BNode)st.getObject();
			object = bn.getID();
			if (!object.startsWith("http"))
				object = "_:" + object;
			type = Environment.OBJECT_PROPERTY; 
		}
		else {
			log.warn("object is not an URI, a literal or a blank node, ignoring " + st);
			return;
		}
		
		String ds = m_defaultContext;
		if (st.getContext() != null) {
			try {
				ds = tldM.getPLD(new URL(st.getContext().toString()));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if (subject != null && object != null && property != null) {
			m_sink.triple(subject, property, object, ds, type);
			m_triplesAdded++;
		}
		else {
			log.debug(subject + " " + property + " " + object);
		}
	}

}