package edu.unika.aifb.ease.util;

import java.util.HashSet;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

public class TypeUtil {
	public static HashSet<String> m_conEdgeSet;
	public static HashSet<String> m_rdfsEdgeSet;
	
	public final static String CONCEPT = "concept";
	public final static String ENTITY = "individual";
	public final static String LITERAL = "literal";
	public final static String PROPERTY = "property";
	public final static String ATTRIBUTE = "datatypeprop";
	public final static String RELATION = "objectprop";
	public final static String TYPE = "type";
	public final static String RDFSPROP = "rdfsprop";
	public final static String SUBCLASS = "subclass";
	public final static String LABEL = "label";
	
	/* edge predefinition */
	private final static String[] conEdges = {RDF.BAG.stringValue(), RDF.SEQ.stringValue(), RDF.ALT.stringValue(), 
			RDF.LIST.stringValue(), RDF.FIRST.stringValue(), RDF.REST.stringValue(), RDF.NIL.stringValue()};

	private final static String[] rdfsEdges = {RDF.TYPE.stringValue(), RDFS.SUBCLASSOF.stringValue(), RDFS.DOMAIN.stringValue(),
			RDFS.RANGE.stringValue(), RDFS.SUBPROPERTYOF.stringValue(), RDFS.LABEL.stringValue(), RDFS.COMMENT.stringValue(),
			RDFS.CLASS.stringValue(), OWL.CLASS.stringValue(), OWL.OBJECTPROPERTY.stringValue(), OWL.DATATYPEPROPERTY.stringValue(),
			RDFS.SEEALSO.stringValue(), RDFS.ISDEFINEDBY.stringValue(), OWL.ONTOLOGY.stringValue(), OWL.ANNOTATIONPROPERTY.stringValue(),
			RDFS.MEMBER.stringValue(), RDFS.CONTAINER.stringValue()};
	
	static {
		m_conEdgeSet = new HashSet<String>();
		for(String edge: conEdges){
			m_conEdgeSet.add(edge);
		}
		
		m_rdfsEdgeSet = new HashSet<String>();
		for(String edge: rdfsEdges){
			m_rdfsEdgeSet.add(edge);
		}
	}
	
	public static String getSubjectType(String pred, String obj) {
		if ((pred.equals(RDF.TYPE.stringValue()) && (obj.equals(RDFS.CLASS.stringValue()) || obj.equals(OWL.CLASS.stringValue())))
				|| pred.equals(RDFS.SUBCLASSOF.stringValue())) {
			return CONCEPT;
		} else if ((pred.equals(RDF.TYPE.stringValue()) && (obj.equals(OWL.OBJECTPROPERTY.stringValue()) || obj.equals(OWL.DATATYPEPROPERTY.stringValue())))
				|| pred.equals(RDFS.DOMAIN.stringValue())
				|| pred.equals(RDFS.RANGE.stringValue())
				|| pred.equals(RDFS.SUBPROPERTYOF.stringValue())) {
			return PROPERTY;
		} else if (pred.equals(RDF.TYPE.stringValue())
				|| getPredicateType(pred, obj).equals(RELATION)
				|| getPredicateType(pred, obj).equals(ATTRIBUTE)) {
			return ENTITY;
		}
		return "";
	}

	public static String getObjectType(String pred, String obj) {
		if (pred.equals(RDF.TYPE.stringValue()) && !m_rdfsEdgeSet.contains(obj)) {
			return CONCEPT;
		} else if (getPredicateType(pred, obj).equals(RELATION)) {
			return ENTITY;
		} else if (getPredicateType(pred, obj).equals(ATTRIBUTE)) {
			return LITERAL;
		}
		return "";
	}

	public static String getPredicateType(String pred, String obj) {
		if (pred.equals(RDFS.LABEL.toString())) 
			return LABEL;
		if (pred.equals(RDF.TYPE.toString())) 
			return TYPE;
		if (m_rdfsEdgeSet.contains(pred)) {
			return RDFSPROP;
		} else if (obj.startsWith("http://") || obj.startsWith("_:")) {
			return RELATION;
		} else if (!obj.startsWith("http://") && !obj.startsWith("_:")) {
			return ATTRIBUTE;
		}
		return "";
	}
	
	public static String checkType(String predicate, String object) {
		String type = null; 
		
		if (TypeUtil.getSubjectType(predicate, object).equals(TypeUtil.ENTITY)
				&& TypeUtil.getObjectType(predicate, object).equals(TypeUtil.ENTITY)
				&& TypeUtil.getPredicateType(predicate, object).equals(TypeUtil.RELATION)) {
			type = TypeUtil.RELATION;
		}
		else if (TypeUtil.getSubjectType(predicate, object).equals(TypeUtil.ENTITY) 
				&& TypeUtil.getObjectType(predicate, object).equals(TypeUtil.LITERAL)
				&& TypeUtil.getPredicateType(predicate, object).equals(TypeUtil.ATTRIBUTE)) {
			type = TypeUtil.ATTRIBUTE;
		}
		else if (TypeUtil.getSubjectType(predicate, object).equals(TypeUtil.ENTITY)
				&& TypeUtil.getObjectType(predicate, object).equals(TypeUtil.CONCEPT)
				&& TypeUtil.getPredicateType(predicate, object).equals(TypeUtil.TYPE)) {
			type = TypeUtil.TYPE;
		}
		else if (predicate.equals(RDFS.LABEL.toString())) {
			type = TypeUtil.LABEL;
		}
		else {
			type = "";
		}
		return type;
	}
	
	public static String getLocalName(String uri) {
		if( uri.lastIndexOf("#") != -1 ) {
			return uri.substring(uri.lastIndexOf("#") + 1);
		}
		else if(uri.lastIndexOf("/") != -1) {
			return uri.substring(uri.lastIndexOf("/") + 1);
		}
		else if(uri.lastIndexOf(":") != -1) {
			return uri.substring(uri.lastIndexOf(":") + 1);
		}
		else {
			return uri;
		}
	}
}
