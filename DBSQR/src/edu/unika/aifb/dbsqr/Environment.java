package edu.unika.aifb.dbsqr;

public class Environment {
	
	/* File types of the data */
	public static final int NTRIPLE = 1;
	public static final int RDFXML = 2;
	public static final int NOTION3 = 3;
	public static final int NQUADS = 4;
	
	/* Types of schema elements */
	public static final int UNKNOWN = 0; 
	public static final int DATA_PROPERTY = 1;
	public static final int OBJECT_PROPERTY = 2;
	public static final int ENTITY_MEMBERSHIP_PROPERTY = 3;
	public static final int RDFS_PROPERTY = 4;
	public static final int CONCEPT = 5;
	
	/* Top Concept */
	public static final String TOP_CONCEPT = "http://www.w3.org/2002/07/owl#Thing"; 
	
	/* Boost of keywords */
	public static final float BOOST_KEYWORD_OF_CONCEPT = 4.0f;
	public static final float BOOST_KEYWORD_OF_OBJECT_PROPERTY = 2.0f;
	public static final float BOOST_KEYWORD_OF_DATA_PROPERTY = 2.0f;
	public static final float BOOST_KEYWORD_OF_DATA_VALUE = 1.0f;
	
	/* Default DbServer information */
	public static final String DEFAULT_SERVER = "localhost";
	public static final String DEFAULT_PORT = "3306";
	public static final String DEFAULT_USERNAME = "root";
	public static final String DEFAULT_PASSWORD = "root";
	public static final String DEFAULT_DATABASE_NAME = "db_dbsqr";
	
	/* Default configuration information */
	public static final int DEFAULT_MAX_DISTANCE = 2;
	public static final int DEFAULT_TOP_KEYWORD = 4;
	public static final int DEFAULT_TOP_DATABASE = 4;
	public static final String DEFAULT_STOPWORD_FILEPATH = "stop_words.txt"; 
	public static final String DEFAULT_TEMPORAL_FILEPATH = "d://dbsqr/lucene";
	
	/* Indexed Field of Entity Index*/
	public static final String FIELD_TERM_LITERAL = "literal";
	public static final String FIELD_TERM_DATAPROPERTY = "dataproperty";
	public static final String FIELD_TERM_OBJECTPROPERTY = "objectproperty";
	public static final String FIELD_TERM_CONCEPT = "concept";
	public static final String FIELD_TERM_LOCALNAME = "localname";
	public static final String FIELD_TERM_LABEL = "label";
	public static final String FIELD_TERM_NAME = "name";
	public static final String FIELD_ENTITY_URI = "entity_uri"; 
	public static final String FIELD_ENTITY_ID = "entity_id";
	public static final String FIELD_CONCEPT_URI = "concept_uri";
	public static final String FIELD_CONCEPT_ID = "concept_id";
	public static final String FIELD_DS = "ds"; 
	public static final String FIELD_DS_ID = "ds_id";
	
	/* Types of keywords */
	public static final int KEYWORD_OF_LITERAL = 1; 
	public static final int KEYWORD_OF_DATA_PROPERTY = 1;
	public static final int KEYWORD_OF_OBJECT_PROPERTY = 2;
	public static final int KEYWORD_OF_CONCEPT = 3;
	public static final int KEYWORD_OF_LOCALNAME = 4;
	public static final int KEYWORD_OF_LABEL = 5;
	public static final int KEYWORD_OF_NAME = 6;
	 
	/* Table names and column names */
	// Triple table
	public static final String TRIPLE_TABLE = "triple_table";					
	public static final String TRIPLE_ID_COLUMN = "triple_id";					// column 1 
	public static final String TRIPLE_SUBJECT_COLUMN = "triple_subject";		// column 2
	public static final String TRIPLE_SUBJECT_ID_COLUMN = "triple_subject_id";	// column 3
	public static final String TRIPLE_PROPERTY_COLUMN = "triple_property";		// column 4
	public static final String TRIPLE_OBJECT_COLUMN = "triple_object";			// column 5
	public static final String TRIPLE_OBJECT_ID_COLUMN = "triple_object_id";	// column 6
	public static final String TRIPLE_PROPERTY_TYPE = "triple_property_type";	// column 7
	public static final String TRIPLE_DS_COLUMN = "triple_ds";					// column 8
	
	// Data source table
	public static final String DATASOURCE_TABLE = "ds_table";								
	public static final String DATASOURCE_ID_COLUMN = "ds_id";   				// column 1 
	public static final String DATASOURCE_NAME_COLUMN = "ds_name";				// column 2
	
	// Schema table
	public static final String SCHEMA_TABLE = "schema_table";	 				
	public static final String SCHEMA_ID_COLUMN = "schema_id";					// column 1 
	public static final String SCHEMA_URI_COLUMN = "schema_uri";				// column 2
	public static final String SCHEMA_TYPE_COLUMN = "schema_type";	 			// column 3
	public static final String SCHEMA_DS_ID_COLUMN = "schema_ds_id";			// column 4
	
	// Entity table
	public static final String ENTITY_TABLE = "entity_table"; 					
	public static final String ENTITY_ID_COLUMN = "entity_id";					// column 1 
	public static final String ENTITY_URI_COLUMN = "entity_uri";				// column 2
	public static final String ENTITY_CONCEPT_ID_COLUMN = "entity_concept_id";  // column 3
	public static final String ENTITY_DS_ID_COLUMN = "entity_ds_id";			// column 4
	public static final String ENTITY_CONCEPT_COLUMN = "entity_concept";		// column 5
	public static final String ENTITY_DS_COLUMN = "entity_ds";					// column 6				
	
	// Entity Concept Membership table
	public static final String ENTITY_CONCEPT_MEMBERSHIP_TABLE = "memb_table";
	public static final String MEMBERSHIP_ENTITY_ID_COLUMN = "memb_entity_id";	// column 1 
	public static final String MEMBERSHIP_CONCEPT_ID_COLUMN = "memb_concept_id";// column 2
	
	// Entity relationship table at distance i 
	public static final String ENTITY_RELATION_TABLE = "entity_rel_table_";	 
	public static final String ENTITY_RELATION_UID_COLUMN = "entity_rel_uid";	// column 1 
	public static final String ENTITY_RELATION_VID_COLUMN = "entity_rel_vid";	// column 2	
	public static final String ENTITY_RELATION_EDGE_ID_COLUMN = "entity_rel_edge";   
	public static final String ENTITY_RELATION_TRIPLE_ID_COLUMN = "entity_rel_triple";
	public static final String ENTITY_RELATION_PATH_COLUMN = "entity_rel_path";  
	// Now we use string type to represent the path, which is a concatenation of the tripleIds.
	// We restrict the max char length of the representation of entity relation path to MAX_LEN_PATH_REP.  
	public static final int MAX_LEN_PATH_REP = 1000;
	
	// Keyword table
	public static final String KEYWORD_TABLE = "keyword_table";					
	public static final String KEYWORD_ID_COLUMN = "keyw_id";	// column 1 
	public static final String KEYWORD_COLUMN = "keyw";			// column 2
	public static final String KEYWORD_TYPE_COLUMN = "keyw_type"; 		// column 3
	
	// Keyword entity inclusion table
	public static final String KEYWORD_ENTITY_INCLUSION_TABLE = "keyw_entity_incl_table";	
	public static final String KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN = "incl_keyw_id";		// column 1	
	public static final String KEYWORD_ENTITY_INCLUSION_KEYWORD_COLUMN = "incl_keyw";			// column 1	
	public static final String KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN = "incl_entity_id";	// column 2
	public static final String KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN = "incl_score"; 			// column 3
	public static final String KEYWORD_ENTITY_INCLUSION_KEYWORD_TYPE_COLUMN = "incl_keyw_type"; // column 4
	public static final String KEYWORD_ENTITY_INCLUSION_CONCEPT_ID_COLUMN = "incl_con_id";		// column 5
	public static final String KEYWORD_ENTITY_INCLUSION_DS_ID_COLUMN = "incl_ds_id";			// column 6 
	public static final String KEYWORD_ENTITY_INCLUSION_PATH_COLUMN = "incl_path"; 				
	
	// Keyword entity reachability table
	public static final String KEYWORD_ENTITY_REACHABILITY_TABLE = "keyw_entity_reach_table";	
	public static final String KEYWORD_ENTITY_REACHABILITY_KEYWORD_ID_COLUMN = "reach_keyw_id";	// column 1	
	public static final String KEYWORD_ENTITY_REACHABILITY_ENTITY_ID_COLUMN = "reach_entity_id";// column 2
	public static final String KEYWORD_ENTITY_REACHABILITY_SCORE_COLUMN = "reach_score"; 		// column 3
	public static final String KEYWORD_ENTITY_REACHABILITY_DS_ID_COLUMN = "reach_ds_id";		// column 4
	public static final String KEYWORD_ENTITY_REACHABILITY_PATH_COLUMN = "reach_path"; 			// column 5
	
	// Keyword pair connected entity table
	public static final String KEYWORDPAIR_CONNECTEDENTITY_TABLE = "keyw_pair_conn_table";
	public static final String KEYWORDPAIR_CONNECTEDENTITY_KEYWORD1_ID_COLUMN = "conn_kId1";	// column 1
	public static final String KEYWORDPAIR_CONNECTEDENTITY_KEYWORD2_ID_COLUMN = "conn_kId1";	// column 2
	public static final String KEYWORDPAIR_CONNECTEDENTITY_ENTITY_ID_COLUMN = "conn_entity_id";	// column 3
	public static final String KEYWORDPAIR_CONNECTEDENTITY_SCORE_COLUMN = "conn_score";			// column 4
	public static final String KEYWORDPAIR_CONNECTEDENTITY_DS_ID_COLUMN = "conn_ds_id";			// column 5

//	// Cluster table
//	public static final String CLUSTER_TABLE = "cluster_table";
//	public static final String CLUSTER_ID_COLUMN = "cluster_id"; 				// column 1
//	public static final String CLUSTER_ELEMENT_ID_COLUMN = "cluster_ele_id"; 	// column 2
//	
//	// Visited Element table (used for clustering)
//	public static final String VISITED_ELEMENT_TABLE = "visited_ele_table";		// column 1
//	public static final String VISITED_ELEMENT_ID_COLUMN = "visited_ele_id";	// column 2
	
	
}

