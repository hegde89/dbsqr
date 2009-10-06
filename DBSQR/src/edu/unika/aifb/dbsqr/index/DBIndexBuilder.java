package edu.unika.aifb.dbsqr.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.dbsqr.Environment;
import edu.unika.aifb.dbsqr.importer.Importer;
import edu.unika.aifb.dbsqr.importer.N3Importer;
import edu.unika.aifb.dbsqr.importer.NxImporter;
import edu.unika.aifb.dbsqr.importer.RDFImporter;
import edu.unika.aifb.dbsqr.importer.TripleSink;
import edu.unika.aifb.dbsqr.util.KeywordTokenizer;
import edu.unika.aifb.dbsqr.util.Stemmer;

public class DBIndexBuilder {
	
	private static final Logger log = Logger.getLogger(DBIndexBuilder.class);
	
	private DbService m_dbService;
	private Map<Integer, Importer> m_importers;
	private DbConfig m_config;
	
	
	public DBIndexBuilder(DbConfig config) {
		this(config, true);
	}
	
	public DBIndexBuilder(DbConfig config, boolean createDb) {
		m_config = config;
		m_importers = new HashMap<Integer, Importer>();
		
		initializeImporters();
		initializeDbService(createDb);	
	}
	
	public void initializeImporters() {
		List<String> filePaths = m_config.getDataFiles();
		for(String filePath : filePaths) {
			File file = new File(filePath);
			List<String> subfilePaths = getSubfilePaths(file);
			for (String subfilePath : subfilePaths) {
				if (subfilePath.contains(".nq") || subfilePath.contains(".nt")) {
					Importer importer = m_importers.get(Environment.NQUADS);
					if (importer == null) {
						importer = new NxImporter();
						m_importers.put(Environment.NQUADS, importer);
					}
					importer.addImport(subfilePath);
				} 
//				if (subfilePath.contains(".nt")) {
//					Importer importer = m_importers.get(Environment.NTRIPLE);
//					if (importer == null) {
//						importer = new NTriplesImporter();
//						m_importers.put(Environment.NTRIPLE, importer);
//					}
//					importer.addImport(subfilePath);
//				} 
				else if (subfilePath.contains(".n3")) {
					Importer importer = m_importers.get(Environment.NOTION3);
					if (importer == null) {
						importer = new N3Importer();
						m_importers.put(Environment.NOTION3, importer);
					}
					importer.addImport(subfilePath);
				} 
				else if (subfilePath.endsWith(".rdf") || subfilePath.endsWith(".xml")) {
					Importer importer = m_importers.get(Environment.RDFXML);
					if (importer == null) {
						importer = new RDFImporter();
						m_importers.put(Environment.RDFXML, importer);
					}
					importer.addImport(subfilePath);
				} 
				else {
					log.warn("unknown extension, assuming n-triples format");
					Importer importer = m_importers.get(Environment.NTRIPLE);
					if (importer == null) {
						importer = new NxImporter();
						m_importers.put(Environment.NTRIPLE, importer);
					}
					importer.addImport(subfilePath);
				}
			}
		}
	} 
	
	public List<String> getSubfilePaths(File file) {
		ArrayList<String> subfilePaths = new ArrayList<String>();
		if (file.isDirectory()) {
			for (File subfile : file.listFiles()) {
				if (!subfile.getName().startsWith(".")) 
					subfilePaths.add(file.getAbsolutePath());
			}	
		}	
		else
			subfilePaths.add(file.getAbsolutePath());
		
		return subfilePaths;
	}
	
	public void initializeDbService(boolean createDb) {
		String server = m_config.getDbServer();
		String username = m_config.getDbUsername();
		String password = m_config.getDbPassword();
		String port = m_config.getDbPort();
		String dbName = m_config.getDbName();
		m_dbService = new DbService(server, username, password, port, dbName, createDb);
	}
	
	public void createTripleTable() {
		log.info("---- Creating Triple Table ----");
		long start = System.currentTimeMillis();
		Statement stmt = m_dbService.createStatement();
		try {
			if (m_dbService.hasTable(Environment.TRIPLE_TABLE)) {
				stmt.execute("drop table " + Environment.TRIPLE_TABLE);
			}
			String createSql = "create table " + Environment.TRIPLE_TABLE + "( " + 
				Environment.TRIPLE_ID_COLUMN + " int unsigned not null primary key auto_increment, " + 
				Environment.TRIPLE_SUBJECT_COLUMN + " varchar(100) not null, " + 
				Environment.TRIPLE_SUBJECT_ID_COLUMN + " int unsigned not null default 0, " + 
				Environment.TRIPLE_PROPERTY_COLUMN + " varchar(100) not null, " + 
				Environment.TRIPLE_OBJECT_COLUMN + " varchar(100) not null, " + 
				Environment.TRIPLE_OBJECT_ID_COLUMN + " int unsigned not null default 0, " + 
				Environment.TRIPLE_PROPERTY_TYPE + " tinyint(1) unsigned not null, " +
				Environment.TRIPLE_DS_COLUMN + " varchar(100) not null) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + " add index (" + Environment.TRIPLE_PROPERTY_TYPE + ")");
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + " add index (" + Environment.TRIPLE_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + " add index (" + Environment.TRIPLE_SUBJECT_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + " add index (" + Environment.TRIPLE_OBJECT_ID_COLUMN + ")");
			
			if(stmt != null)
				stmt.close();
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating triple table:");
			log.warn(ex.getMessage());
		} 
		
		log.info("---- Importing Triples into Triple Table ----");
		DbTripleSink sink = new DbTripleSink();
		for(Importer importer : m_importers.values()) {
			importer.setTripleSink(sink);
			importer.doImport();
		}
		sink.close();
		
		long end = System.currentTimeMillis();
		log.info("Time for Creating Triple Table: " + (double)(end - start)/(double)1000 + "(sec)");
	}
	
	public void createDatasourceTable() {
		log.info("---- Creating Datasource Table ----");
		long start = System.currentTimeMillis();
		Statement stmt = m_dbService.createStatement();
		try {
			if (m_dbService.hasTable(Environment.DATASOURCE_TABLE)) {
				stmt.execute("drop table " + Environment.DATASOURCE_TABLE);
			}
			String createSql = "create table " + 
				Environment.DATASOURCE_TABLE + "( " + 
				Environment.DATASOURCE_ID_COLUMN + " smallint unsigned not null primary key auto_increment, " + 
				Environment.DATASOURCE_NAME_COLUMN + " varchar(100) not null, " + 
				" index(" + Environment.DATASOURCE_ID_COLUMN + "), " + 
				" index(" + Environment.DATASOURCE_NAME_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
		
			log.info("---- Populating Datasource Table ----");
			String insertSql = "insert into " + Environment.DATASOURCE_TABLE + "(" + 
				Environment.DATASOURCE_NAME_COLUMN + ") ";
			
			String selectSql = "select distinct " + 
				Environment.TRIPLE_DS_COLUMN +
				" from " + Environment.TRIPLE_TABLE; 
			stmt.executeUpdate(insertSql + selectSql);
			
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Datasource Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating datasource table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createSchemaTable() {
		log.info("---- Creating Schema Table ----");
		long start = System.currentTimeMillis();
        Statement stmt = m_dbService.createStatement();
        try {
			if (m_dbService.hasTable(Environment.SCHEMA_TABLE)) {
				stmt.execute("drop table " + Environment.SCHEMA_TABLE);
			}
			String createSql = "create table " + 
				Environment.SCHEMA_TABLE + "( " + 
				Environment.SCHEMA_ID_COLUMN + " mediumint unsigned not null primary key auto_increment, " + 
				Environment.SCHEMA_URI_COLUMN + " varchar(100) not null, " + 
				Environment.SCHEMA_TYPE_COLUMN + " tinyint(1) unsigned not null, " +
				Environment.SCHEMA_DS_ID_COLUMN + " smallint unsigned not null default 0, " + 
				"index(" + Environment.SCHEMA_ID_COLUMN + "), " + 
				"index(" + Environment.SCHEMA_TYPE_COLUMN + "), " + 
				"index(" + Environment.SCHEMA_URI_COLUMN + ")) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			
			log.info("---- Populating Schema Table ----");
			String insertSql = "insert into " + Environment.SCHEMA_TABLE + "(" + 
				Environment.SCHEMA_URI_COLUMN + ", " +
				Environment.SCHEMA_TYPE_COLUMN + ", " + 
				Environment.SCHEMA_DS_ID_COLUMN + ") ";
			
			String selectSql = "select distinct " + 
				Environment.TRIPLE_OBJECT_COLUMN + ", " + 
				Environment.CONCEPT + ", " +
				Environment.DATASOURCE_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + ", " + Environment.DATASOURCE_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.ENTITY_MEMBERSHIP_PROPERTY + 
				" and " +  Environment.TRIPLE_DS_COLUMN + " = " + Environment.DATASOURCE_NAME_COLUMN;
			stmt.executeUpdate(insertSql + selectSql);
			
//			selectSql = "select distinct " + 
//				Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
//				Environment.OBJECT_PROPERTY + ", " +
//				Environment.DATASOURCE_ID_COLUMN +
//				" from " + Environment.TRIPLE_TABLE + ", " + Environment.DATASOURCE_TABLE + 
//				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
//				" and " +  Environment.TRIPLE_DS_COLUMN + " = " + Environment.DATASOURCE_NAME_COLUMN;
//			stmt.executeUpdate(insertSql + selectSql);
//			
//			selectSql = "select distinct " + 
//				Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
//				Environment.DATA_PROPERTY + ", " +
//				Environment.DATASOURCE_ID_COLUMN +
//				" from " + Environment.TRIPLE_TABLE + ", " + Environment.DATASOURCE_TABLE + 
//				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.DATA_PROPERTY + 
//				" and " +  Environment.TRIPLE_DS_COLUMN + " = " + Environment.DATASOURCE_NAME_COLUMN;
//			stmt.executeUpdate(insertSql + selectSql);
			
			insertSql = "insert into " + Environment.SCHEMA_TABLE + "(" + 
				Environment.SCHEMA_URI_COLUMN + ", " +
				Environment.SCHEMA_TYPE_COLUMN + ") ";  
		
			selectSql = "select distinct " + 
				Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
				Environment.OBJECT_PROPERTY + 
				" from " + Environment.TRIPLE_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY; 
			stmt.executeUpdate(insertSql + selectSql);
		
			selectSql = "select distinct " + 
				Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
				Environment.DATA_PROPERTY + 
				" from " + Environment.TRIPLE_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.DATA_PROPERTY; 
			stmt.executeUpdate(insertSql + selectSql);
			
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Schema Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating schema table:");
			log.warn(ex.getMessage());
		}  
	}
	
	public void createEntityTable() {
		log.info("---- Creating Entity Table ----");
        Statement stmt = m_dbService.createStatement();
        long start = System.currentTimeMillis();
        try {
			if (m_dbService.hasTable(Environment.ENTITY_TABLE)) {
				stmt.execute("drop table " + Environment.ENTITY_TABLE);
			}
			String createSql = "create table " + 
				Environment.ENTITY_TABLE + "( " + 
				Environment.ENTITY_ID_COLUMN + " int unsigned not null primary key auto_increment, " + 
				Environment.ENTITY_URI_COLUMN + " varchar(100) not null, " + 
				Environment.ENTITY_CONCEPT_ID_COLUMN + " mediumint unsigned, " +
				Environment.ENTITY_DS_ID_COLUMN + " smallint unsigned not null, " + 
				Environment.ENTITY_CONCEPT_COLUMN + " varchar(100), " + 
				Environment.ENTITY_DS_COLUMN + " varchar(100) not null) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.ENTITY_TABLE + " add index (" + Environment.ENTITY_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.ENTITY_TABLE + " add index (" + Environment.ENTITY_URI_COLUMN + ")");
			
			log.info("---- Populating Entity Table ----");
			String insertSql = "insert into " + Environment.ENTITY_TABLE + "(" + 
				Environment.ENTITY_URI_COLUMN + ", " + Environment.ENTITY_DS_ID_COLUMN + ", " + Environment.ENTITY_DS_COLUMN + ") "; 
			String selectSql = 	"select distinct " + 
				Environment.TRIPLE_SUBJECT_COLUMN + ", " + Environment.DATASOURCE_ID_COLUMN + ", " + Environment.TRIPLE_DS_COLUMN +
				" from " + Environment.TRIPLE_TABLE + ", " + Environment.DATASOURCE_TABLE +
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " <> " + Environment.RDFS_PROPERTY + 
				" and " + Environment.TRIPLE_DS_COLUMN + " = " + Environment.DATASOURCE_NAME_COLUMN +
				" union distinct " + "select distinct " + 
				Environment.TRIPLE_OBJECT_COLUMN + ", " + Environment.DATASOURCE_ID_COLUMN + ", " + Environment.TRIPLE_DS_COLUMN +
				" from " + Environment.TRIPLE_TABLE + ", " + Environment.DATASOURCE_TABLE +
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY +
				" and " + Environment.TRIPLE_DS_COLUMN + " = " + Environment.DATASOURCE_NAME_COLUMN;
			String insertSelectSql = insertSql + selectSql;
			stmt.executeUpdate(insertSelectSql);
			
			String updateSql = "update " + Environment.TRIPLE_TABLE + " as A, " + 
				Environment.SCHEMA_TABLE + " as B, " + Environment.ENTITY_TABLE + " as C " +
				" set " + "C." + Environment.ENTITY_CONCEPT_ID_COLUMN + " = " + "B." + Environment.SCHEMA_ID_COLUMN + ", " +
				"C." + Environment.ENTITY_DS_ID_COLUMN + " = " + "B." + Environment.SCHEMA_DS_ID_COLUMN + ", " +
				"C." + Environment.ENTITY_CONCEPT_COLUMN + " = " + "B." + Environment.SCHEMA_URI_COLUMN + 
				" where " + "A." + Environment.TRIPLE_SUBJECT_COLUMN + " = " + "C." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_OBJECT_COLUMN + " = " + "B." + Environment.SCHEMA_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.ENTITY_MEMBERSHIP_PROPERTY + 
				" and " + "B." + Environment.SCHEMA_TYPE_COLUMN + " = " + Environment.CONCEPT; 
			stmt.executeUpdate(updateSql);
			
			updateSql = "update " + Environment.TRIPLE_TABLE + " as A, " + Environment.ENTITY_TABLE + " as B " +
				" set " + "A." + Environment.TRIPLE_SUBJECT_ID_COLUMN + " = " + "B." + Environment.ENTITY_ID_COLUMN + 
				" where " + "A." + Environment.TRIPLE_SUBJECT_COLUMN + " = " + "B." + Environment.ENTITY_URI_COLUMN;
			stmt.executeUpdate(updateSql);
			 
			updateSql = "update " + Environment.TRIPLE_TABLE + " as A, " + 	Environment.ENTITY_TABLE + " as B " +
				" set " + "A." + Environment.TRIPLE_OBJECT_ID_COLUMN + " = " + "B." + Environment.ENTITY_ID_COLUMN + 
				" where " + "A." + Environment.TRIPLE_OBJECT_COLUMN + " = " + "B." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY; 
			stmt.executeUpdate(updateSql);
			
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Entity Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createEntityRelationTable() {
		log.info("---- Creating Entity Relation Table ----");
		long start = System.currentTimeMillis();
        Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        String R_1 = Environment.ENTITY_RELATION_TABLE + 1; 
        try {
        	// Create Entity Relation Table 
			if (m_dbService.hasTable(R_1)) {
				stmt.execute("drop table " + R_1);
			}
			String createSql = "create table " + R_1 + "( " + 
				Environment.ENTITY_RELATION_UID_COLUMN + " int unsigned not null, " + 
				Environment.ENTITY_RELATION_VID_COLUMN + " int unsigned not null, " +
				"primary key("	+ Environment.ENTITY_RELATION_UID_COLUMN + ", " + Environment.ENTITY_RELATION_VID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + R_1 + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")");
			stmt.execute("alter table " + R_1 + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")");
			
			log.info("---- Populating Entity Relation Table ----");
			// Populate Entity Relation Table 
			String insertSql = "insert IGNORE into " + R_1 + " values(?, ?)"; 
			PreparedStatement ps = m_dbService.createPreparedStatement(insertSql,ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
			String selectSql = "select " + Environment.TRIPLE_SUBJECT_ID_COLUMN + ", " + Environment.TRIPLE_OBJECT_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE +  
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
				" group by " + Environment.TRIPLE_SUBJECT_ID_COLUMN + ", " + Environment.TRIPLE_OBJECT_ID_COLUMN;  
			ResultSet rs = stmt.executeQuery(selectSql);
            while (rs.next()){
            	int entityId1 = rs.getInt(1);
            	int entityId2 = rs.getInt(2);
            	if(entityId1 < entityId2){
            		ps.setInt(1, entityId1);
            		ps.setInt(2, entityId2);
            	}else{
            		ps.setInt(1, entityId2);
            		ps.setInt(2, entityId1);
            	}
                ps.executeUpdate();
            }
            if(rs != null)
            	rs.close();
            if(ps != null)
            	ps.close();
            
			if(stmt != null)
            	stmt.close();
            
            long end = System.currentTimeMillis();
			log.info("Time for Creating Entity Relation Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity relation table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createEntityRelationTableWithEdges() {
		log.info("---- Creating Entity Relation Table ----");
		long start = System.currentTimeMillis();
        Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        String R_1 = Environment.ENTITY_RELATION_TABLE + 1; 
        try {
        	// Create Entity Relation Table 
			if (m_dbService.hasTable(R_1)) {
				stmt.execute("drop table " + R_1);
			}
			String createSql = "create table " + R_1 + "( " + 
				Environment.ENTITY_RELATION_UID_COLUMN + " int unsigned not null, " + 
				Environment.ENTITY_RELATION_VID_COLUMN + " int unsigned not null, " +
				Environment.ENTITY_RELATION_EDGE_ID_COLUMN + " mediumint unsigned not null) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + R_1 + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")");
			stmt.execute("alter table " + R_1 + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")");
			
			log.info("---- Populating Entity Relation Table ----");
			// Populate Entity Relation Table 
			String insertSql = "insert IGNORE into " + R_1 + " values(?, ?, ?)"; 
			PreparedStatement ps = m_dbService.createPreparedStatement(insertSql,ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
			String selectSql = "select distinct " + "B." + Environment.ENTITY_ID_COLUMN + ", " + 
				"C." + Environment.ENTITY_ID_COLUMN + ", " + "D." + Environment.SCHEMA_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + " as A, " + Environment.ENTITY_TABLE + " as B, " + 
				Environment.ENTITY_TABLE + " as C, " + Environment.SCHEMA_TABLE + " as D " +
				" where " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
				" and " + "A." + Environment.TRIPLE_SUBJECT_COLUMN + " = " + "B." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_OBJECT_COLUMN + " = " + "C." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_PROPERTY_COLUMN + " = " + "D." + Environment.SCHEMA_URI_COLUMN + 
				" and " + "D." + Environment.SCHEMA_TYPE_COLUMN + " = " + Environment.OBJECT_PROPERTY;
			ResultSet rs = stmt.executeQuery(selectSql);
            while (rs.next()){
            	int entityId1 = rs.getInt(1);
            	int entityId2 = rs.getInt(2);
            	int edgeId = rs.getInt(3);
            	ps.setInt(1, entityId1);
            	ps.setInt(2, entityId2);
            	ps.setInt(3, edgeId);
                ps.executeUpdate();
            }
            if(rs != null)
            	rs.close();
            if(ps != null)
            	ps.close();
            
			if(stmt != null)
            	stmt.close();
            
            long end = System.currentTimeMillis();
			log.info("Time for Creating Entity Relation Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity relation table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createEntityRelationTableWithTriples() {
		log.info("---- Creating Entity Relation Table ----");
		long start = System.currentTimeMillis();
        Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        String entityRelationtTable_1 = Environment.ENTITY_RELATION_TABLE + 1; 
        try {
        	// Create Entity Relation Table 
			if (m_dbService.hasTable(entityRelationtTable_1)) {
				stmt.execute("drop table " + entityRelationtTable_1);
			}
			String createSql = "create table " + entityRelationtTable_1 + "( " + 
				Environment.ENTITY_RELATION_UID_COLUMN + " int unsigned not null, " + 
				Environment.ENTITY_RELATION_VID_COLUMN + " int unsigned not null, " +
				Environment.ENTITY_RELATION_TRIPLE_ID_COLUMN + " int unsigned not null) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + entityRelationtTable_1 + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")");
			stmt.execute("alter table " + entityRelationtTable_1 + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")");
			
			log.info("---- Populating Entity Relation Table ----");
			// Populate Temporal Entity Relation Table 
			String insertSql = "insert IGNORE into " + entityRelationtTable_1 + " values(?, ?, ?)"; 
			PreparedStatement ps = m_dbService.createPreparedStatement(insertSql,ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
			String selectSql = "select distinct " + "B." + Environment.ENTITY_ID_COLUMN + ", " + 
				"C." + Environment.ENTITY_ID_COLUMN + ", " + "D." + Environment.SCHEMA_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + " as A, " + Environment.ENTITY_TABLE + " as B, " + 
				Environment.ENTITY_TABLE + " as C, " + Environment.SCHEMA_TABLE + " as D " +
				" where " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
				" and " + "A." + Environment.TRIPLE_SUBJECT_COLUMN + " = " + "B." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_OBJECT_COLUMN + " = " + "C." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_PROPERTY_COLUMN + " = " + "D." + Environment.SCHEMA_URI_COLUMN + 
				" and " + "D." + Environment.SCHEMA_TYPE_COLUMN + " = " + Environment.OBJECT_PROPERTY;
			ResultSet rs = stmt.executeQuery(selectSql);
            while (rs.next()){
            	int entityId1 = rs.getInt(1);
            	int entityId2 = rs.getInt(2);
            	int edgeId = rs.getInt(3);
            	ps.setInt(1, entityId1);
            	ps.setInt(2, entityId2);
            	ps.setInt(3, edgeId);
                ps.executeUpdate();
            }
            if(rs != null)
            	rs.close();
            if(ps != null)
            	ps.close();
            
			if(stmt != null)
            	stmt.close();
            
            long end = System.currentTimeMillis();
			log.info("Time for Creating Entity Relation Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity relation table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createEntityRelationTable(int distance) {
		log.info("---- Creating Entity Relation Table at distance " + distance + " ----");
		long start = System.currentTimeMillis();
        Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        String R_d = Environment.ENTITY_RELATION_TABLE + distance; 
        try {
        	// Create Entity Relation Table at distance d
			if (m_dbService.hasTable(R_d)) {
				stmt.execute("drop table " + R_d);
			}
			String createSql = "create table " + R_d + "( " + 
				Environment.ENTITY_RELATION_UID_COLUMN + " int unsigned not null, " + 
				Environment.ENTITY_RELATION_VID_COLUMN + " int unsigned not null, " +
				"primary key("	+ Environment.ENTITY_RELATION_UID_COLUMN + ", " + Environment.ENTITY_RELATION_VID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + R_d + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")");
			stmt.execute("alter table " + R_d + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")");
			
			log.info("---- Populating Entity Relation Table at distance " + distance + " ----");
			// Populate Temporal Entity Relation Table at distance d
			int num = 0;
			if(distance == 2){
				String insertSql = "insert IGNORE into " + R_d + " values(?, ?)"; 
				PreparedStatement ps = m_dbService.createPreparedStatement(insertSql, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
				String R_1 = Environment.ENTITY_RELATION_TABLE + 1;
				
				// R_1(u, v), R_1(u', v') -> R_2(u, v') where v = u'
				String selectSql = 	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN +
					" from " + R_1 + " as A, " + R_1 + " as B " +
					" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + 
					" group by " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN;  
				ResultSet rs = stmt.executeQuery(selectSql);
				while (rs.next()){
					if(++num % 1000 == 0)
						log.info(num);
					int entityId1 = rs.getInt(1);
					int entityId2 = rs.getInt(2);
					ps.setInt(1, entityId1);
					ps.setInt(2, entityId2);
					ps.executeUpdate();
				}
				if(rs != null)
					rs.close();
				
				// R_1(u, v), R_1(u', v') -> R_2(u, u') where v = v'
				selectSql =	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_UID_COLUMN +
					" from " + R_1 + " as A, " + R_1 + " as B " +
					" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + "B." + Environment.ENTITY_RELATION_VID_COLUMN + 
					" and " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " < " + "B." + Environment.ENTITY_RELATION_UID_COLUMN +
					" group by " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_UID_COLUMN;  
				rs = stmt.executeQuery(selectSql);
				while (rs.next()){
					if(++num % 1000 == 0)
						log.info(num);
					int entityId1 = rs.getInt(1);
					int entityId2 = rs.getInt(2);
					ps.setInt(1, entityId1);
					ps.setInt(2, entityId2);
					ps.executeUpdate();
				}
				if(rs != null)
					rs.close();
				
				// R_1(u, v), R_1(u', v') -> R_2(v, v') where u = u'
				selectSql =	"select " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN +
					" from " + R_1 + " as A, " + R_1 + " as B " +
					" where " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + " and " +
					"A." + Environment.ENTITY_RELATION_VID_COLUMN + " < " + "B." + Environment.ENTITY_RELATION_VID_COLUMN +
					" group by " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN;  
				rs = stmt.executeQuery(selectSql);
				while (rs.next()) {
					if(++num % 1000 == 0)
						log.info(num);
					int entityId1 = rs.getInt(1);
					int entityId2 = rs.getInt(2);
					ps.setInt(1, entityId1);
					ps.setInt(2, entityId2);
					ps.executeUpdate();
				}
				if(rs != null)
					rs.close();
				if(ps != null)
					ps.close();
				
				int deletedRows = 0;
				for(int i=1; i<distance; i++){
					String R_i = Environment.ENTITY_RELATION_TABLE + i;
					String deleteSql = "delete " + R_d + " from " + R_d + ", " + R_i +
						" where " + R_d + "." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
						R_i + "." + Environment.ENTITY_RELATION_UID_COLUMN + " and " + 
						R_d + "." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
						R_i + "." + Environment.ENTITY_RELATION_VID_COLUMN;
					deletedRows += stmt.executeUpdate(deleteSql);
				}
				log.info("Number of duplicated rows that are deleted: " + deletedRows);
			}
			
			if(distance >= 3){	
				String insertSql = "insert IGNORE into " + R_d + " values(?, ?)"; 
				PreparedStatement ps = m_dbService.createPreparedStatement(insertSql);
				String R_1 = Environment.ENTITY_RELATION_TABLE + 1;
				String R_d_minus_1 = Environment.ENTITY_RELATION_TABLE + (distance - 1);
				
				// R_(d-1)(u, v), R_1(u', v') -> R_d(u, v') where v = u'
				String selectSql = 	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN + 
					" from " + R_d_minus_1 + " as A, " + R_1 + " as B " +
					" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + 
					" group by " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN;  
				ResultSet rs = stmt.executeQuery(selectSql);
				while (rs.next()){
					if(++num % 1000 == 0)
						log.info(num);
					int entityId1 = rs.getInt(1);
					int entityId2 = rs.getInt(2);
					ps.setInt(1, entityId1);
					ps.setInt(2, entityId2);
					ps.executeUpdate();
				}
				if(rs != null)
					rs.close();
				
				// R_(d-1)(u, v), R_1(u', v') -> R_d(u', v) where u = v'
				selectSql = "select " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + 
					" from " + R_d_minus_1 + " as A, " + R_1 + " as B " +
					" where " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + "B." + Environment.ENTITY_RELATION_VID_COLUMN + 
					" group by " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "A." + Environment.ENTITY_RELATION_VID_COLUMN;  
				rs = stmt.executeQuery(selectSql);
				while (rs.next()) {
					if(++num % 1000 == 0)
						log.info(num);
					int entityId1 = rs.getInt(1);
					int entityId2 = rs.getInt(2);
					ps.setInt(1, entityId1);
					ps.setInt(2, entityId2);
					ps.executeUpdate();
				}
				if (rs != null)
					rs.close();
				
				// R_(d-1)(u, v), R_1(u', v') -> R_d(u, u') where v = v'
				selectSql =	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + 
					" from " + R_d_minus_1 + " as A, " + R_1 + " as B " +
					" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + "B." + Environment.ENTITY_RELATION_VID_COLUMN + " and " +
					"A." + Environment.ENTITY_RELATION_UID_COLUMN + " < " + "B." + Environment.ENTITY_RELATION_UID_COLUMN +
					" group by " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_UID_COLUMN;  
				rs = stmt.executeQuery(selectSql);
				while (rs.next()){
					if(++num % 1000 == 0)
						log.info(num);
					int entityId1 = rs.getInt(1);
					int entityId2 = rs.getInt(2);
					ps.setInt(1, entityId1);
					ps.setInt(2, entityId2);
					ps.executeUpdate();
				}
				if(rs != null)
					rs.close();
				
				// R_(d-1)(u, v), R_1(u', v') -> R_d(v, v') where u = u'
				selectSql =	"select " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN + 
					" from " + R_d_minus_1 + " as A, " + R_1 + " as B " +
					" where " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + " and " +
					"A." + Environment.ENTITY_RELATION_VID_COLUMN + " < " + "B." + Environment.ENTITY_RELATION_VID_COLUMN +
					" group by " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN;  
				rs = stmt.executeQuery(selectSql);
				while (rs.next()) {
					if(++num % 1000 == 0)
						log.info(num);
					int entityId1 = rs.getInt(1);
					int entityId2 = rs.getInt(2);
					ps.setInt(1, entityId1);
					ps.setInt(2, entityId2);
					ps.executeUpdate();
				}
				if(rs != null)
					rs.close();
				if(ps != null)
					ps.close();
	
				int deletedRows = 0;
				for(int i=1; i<distance; i++){
					String R_i = Environment.ENTITY_RELATION_TABLE + i;
					String deleteSql = "delete " + R_d + " from " + R_d + ", " + R_i +
						" where " + R_d + "." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
						R_i + "." + Environment.ENTITY_RELATION_UID_COLUMN + " and " + 
						R_d + "." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
						R_i + "." + Environment.ENTITY_RELATION_VID_COLUMN;
					deletedRows += stmt.executeUpdate(deleteSql);
				}
				log.info("Number of duplicated rows that are deleted: " + deletedRows);
			}
			
			stmt.execute("flush tables");
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Entity Relation Table at distance " + distance + ": " + (double)(end - start)/(double)60000 + "(min)");
			
			System.gc();
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity relation table at distance " + distance + ":");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createKeywordLuceneIndex() {
		log.info("---- Creating Keyword Index ----");
		long start = System.currentTimeMillis();
		
		String indexPath = m_config.getTemporaryDirectory();
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
//		Analyzer analyzer = new WhitespaceAnalyzer();
		// Store the index on disk:
		File dir = new File(indexPath);
		if(!dir.exists())
			dir.mkdirs();
		Directory directory;
		
        Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
		
		try {
			directory = FSDirectory.open(dir);
			IndexWriter iwriter = new IndexWriter(directory, analyzer, true, new IndexWriter.MaxFieldLength(25000));
			
			// Statement for Entity Table
			String selectEntitySql = "select " + Environment.ENTITY_ID_COLUMN + ", " + Environment.ENTITY_URI_COLUMN + ", " + 
				Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + Environment.ENTITY_DS_ID_COLUMN + ", " + 
				Environment.ENTITY_DS_COLUMN + ", " + Environment.ENTITY_CONCEPT_COLUMN + 
				" from " + Environment.ENTITY_TABLE;
			ResultSet rsEntity = stmt.executeQuery(selectEntitySql);
			
			// Statement for Triple Table
			String selectTripleSqlFw = "select " + Environment.TRIPLE_PROPERTY_TYPE + ", " + Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
				Environment.TRIPLE_OBJECT_COLUMN + 
				" from " + Environment.TRIPLE_TABLE +
				" where " + Environment.TRIPLE_SUBJECT_ID_COLUMN + " = ?";
			PreparedStatement psQueryTripleFw = m_dbService.createPreparedStatement(selectTripleSqlFw);
			ResultSet rsTriple = null;
			
			int numEntities = 0;
			// processing each entity
			while(rsEntity.next()) {
				numEntities++;
				if(numEntities % 1000 == 0)
					log.info("Processed Entities: " + numEntities);
				String entityUri = rsEntity.getString(Environment.ENTITY_URI_COLUMN);
				int nEntityId = rsEntity.getInt(Environment.ENTITY_ID_COLUMN);
				String entityId = String.valueOf(nEntityId);
				String ds = rsEntity.getString(Environment.ENTITY_DS_COLUMN); 
				String dsId = String.valueOf(rsEntity.getInt(Environment.ENTITY_DS_ID_COLUMN));
				int nConceptId = rsEntity.getInt(Environment.ENTITY_CONCEPT_ID_COLUMN);
				String conceptId = "";
				String conceptUri = (rsEntity.getString(Environment.ENTITY_CONCEPT_COLUMN) == null) ? "" : rsEntity.getString(Environment.ENTITY_CONCEPT_COLUMN);
				if(!(conceptId == null))
					conceptId = String.valueOf(nConceptId);
				String termsOfLocalname = trucateUri(entityUri);
				String termsOfLiterals = "";
				String termsOfDataProperties = "";
				String termsOfObjectProperties = "";
				String termsOfConcepts = "";
				String termsOfLabels = "";
				String termsOfNames = "";
				// processing forward edges
				psQueryTripleFw.setInt(1, nEntityId);
				rsTriple = psQueryTripleFw.executeQuery();
				while (rsTriple.next()) {
					int type = rsTriple.getInt(Environment.TRIPLE_PROPERTY_TYPE);
					if(type == Environment.DATA_PROPERTY) {
						// term for data property 
						String dataProperty = rsTriple.getString(Environment.TRIPLE_PROPERTY_COLUMN);
						if(!dataProperty.startsWith(RDF.NAMESPACE) && !dataProperty.startsWith(RDFS.NAMESPACE)) {
							termsOfDataProperties += trucateUri(dataProperty) + " ";
						}
						// term for literal
						if(dataProperty.equals(RDFS.LABEL.stringValue())) {
							termsOfLabels += rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN) + " ";
						}
						else if(trucateUri(dataProperty).contains("name")) {
							termsOfNames += rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN) + " ";
						}
						else {
							termsOfLiterals += rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN) + " ";
						}
					}
					else if(type == Environment.OBJECT_PROPERTY) {
						// term for object property 
						String objectProperty = rsTriple.getString(Environment.TRIPLE_PROPERTY_COLUMN); 
						if(!objectProperty.startsWith(RDF.NAMESPACE) && !objectProperty.startsWith(RDFS.NAMESPACE)) {
							termsOfObjectProperties += trucateUri(objectProperty) + " ";
						}
					}
					else if(type == Environment.ENTITY_MEMBERSHIP_PROPERTY) {
						// term for concept 
						String concept = rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN);
						if(!concept.startsWith(RDF.NAMESPACE) && !concept.startsWith(RDFS.NAMESPACE)) {
							termsOfConcepts += trucateUri(concept) + " ";
						}
					}
				}
				if(rsTriple != null)
					rsTriple.close();
				
				Document doc = new Document();
				doc.add(new Field(Environment.FIELD_ENTITY_URI, entityUri, Field.Store.YES, Field.Index.NO));
				doc.add(new Field(Environment.FIELD_ENTITY_ID, entityId, Field.Store.YES, Field.Index.NO));
				doc.add(new Field(Environment.FIELD_DS, ds, Field.Store.YES, Field.Index.NO));
				doc.add(new Field(Environment.FIELD_DS_ID, dsId, Field.Store.YES, Field.Index.NO));
				if(!conceptId.equals(""))
					doc.add(new Field(Environment.FIELD_CONCEPT_ID, conceptId, Field.Store.YES, Field.Index.NO));
				if(!conceptUri.equals(""))
					doc.add(new Field(Environment.FIELD_CONCEPT_URI, conceptUri, Field.Store.YES, Field.Index.NO));
				if(!termsOfLiterals.equals(""))
					doc.add(new Field(Environment.FIELD_TERM_LITERAL, termsOfLiterals, Field.Store.YES, Field.Index.ANALYZED));
				if(!termsOfLocalname.equals(""))
					doc.add(new Field(Environment.FIELD_TERM_LOCALNAME, termsOfLocalname, Field.Store.YES, Field.Index.ANALYZED));
				if(!termsOfConcepts.equals(""))
					doc.add(new Field(Environment.FIELD_TERM_CONCEPT, termsOfConcepts, Field.Store.YES, Field.Index.ANALYZED));
				if(!termsOfDataProperties.equals(""))
					doc.add(new Field(Environment.FIELD_TERM_DATAPROPERTY, termsOfDataProperties, Field.Store.YES, Field.Index.ANALYZED));
				if(!termsOfObjectProperties.equals(""))
					doc.add(new Field(Environment.FIELD_TERM_OBJECTPROPERTY, termsOfObjectProperties, Field.Store.YES, Field.Index.ANALYZED));
				if(!termsOfLabels.equals(""))
					doc.add(new Field(Environment.FIELD_TERM_LABEL, termsOfLabels, Field.Store.YES, Field.Index.ANALYZED));
				if(!termsOfNames.equals(""))
					doc.add(new Field(Environment.FIELD_TERM_NAME, termsOfNames, Field.Store.YES, Field.Index.ANALYZED));
				iwriter.addDocument(doc);
			}	
			
			if(rsEntity != null)
				rsEntity.close();
			if(psQueryTripleFw != null)
				psQueryTripleFw.close();
			if(stmt != null)
				stmt.close();
			
			iwriter.close();
			directory.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Keyword Index: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createKeywordEntityInclusionTableUsingLucene() {
		// construct keyword index using Lucene
		createKeywordLuceneIndex();
		log.info("---- Creating Keyword Entity Inclusion Table and Keyword Table ----");
		long start = System.currentTimeMillis();
		
		Statement stmt = m_dbService.createStatement();
		try {
			if (m_dbService.hasTable(Environment.KEYWORD_TABLE)) {
				stmt.execute("drop table " + Environment.KEYWORD_TABLE);
			}
			String createSql = "create table " + Environment.KEYWORD_TABLE + "( " + 
				Environment.KEYWORD_ID_COLUMN + " int unsigned not null primary key, " + 
				Environment.KEYWORD_COLUMN + " varchar(100) not null, " + 
				Environment.KEYWORD_TYPE_COLUMN + " tinyint(1) unsigned not null) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.KEYWORD_TABLE + " add index (" + Environment.KEYWORD_COLUMN + ")");
			
			if (m_dbService.hasTable(Environment.KEYWORD_ENTITY_INCLUSION_TABLE)) {
				stmt.execute("drop table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE);
			}
			createSql = "create table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + "( " + 
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " float unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_TYPE_COLUMN + " tinyint(1) unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_CONCEPT_ID_COLUMN + " mediumint unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_DS_ID_COLUMN + " smallint unsigned not null, " + 
				"primary key(" + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ", " +
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_TYPE_COLUMN + ", " +
				Environment.KEYWORD_ENTITY_INCLUSION_DS_ID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			
			log.info("---- Populating Keyword Entity Inclusion Table and Keyword Table ----");
			// Statement for Keyword Table
			String insertKeywSql = "insert into " + Environment.KEYWORD_TABLE + " values(?, ?, ?)"; 
			PreparedStatement psInsertKeyw = m_dbService.createPreparedStatement(insertKeywSql);
			
			// Statement for Keyword Entity Inclusion Table
			String insertKeywEntitySql = "insert IGNORE into " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " values(?, ?, ?, ?, ?, ?)"; 
			PreparedStatement psInsertKeywEntity = m_dbService.createPreparedStatement(insertKeywEntitySql);
			
			// Retrieve Keywords from Lucene Index
			Directory directory = FSDirectory.open(new File(m_config.getTemporaryDirectory()));
			IndexReader ireader = IndexReader.open(directory, true);
			
			int numDocs = ireader.numDocs();
			
			String[] loadFields = {Environment.FIELD_ENTITY_ID, Environment.FIELD_CONCEPT_ID, Environment.FIELD_DS_ID};
			MapFieldSelector fieldSelector = new MapFieldSelector(loadFields);
			Map<String,Integer> keywordTypes = new HashMap<String,Integer>();
			keywordTypes.put(Environment.FIELD_TERM_LITERAL, Environment.KEYWORD_OF_LITERAL);
			keywordTypes.put(Environment.FIELD_TERM_DATAPROPERTY, Environment.KEYWORD_OF_DATA_PROPERTY);
			keywordTypes.put(Environment.FIELD_TERM_OBJECTPROPERTY, Environment.KEYWORD_OF_OBJECT_PROPERTY);
			keywordTypes.put(Environment.FIELD_TERM_CONCEPT, Environment.KEYWORD_OF_CONCEPT);
			keywordTypes.put(Environment.FIELD_TERM_LOCALNAME, Environment.KEYWORD_OF_LOCALNAME);
			keywordTypes.put(Environment.FIELD_TERM_LABEL, Environment.KEYWORD_OF_LABEL);
			keywordTypes.put(Environment.FIELD_TERM_NAME, Environment.KEYWORD_OF_NAME);
			
			// For Test
			PrintWriter pw = new PrintWriter("d://keyword.txt"); 
			
			int keywordId = 0;
			TermEnum tEnum = ireader.terms();
			while(tEnum.next()) {
				keywordId++;
				if(keywordId % 1000 == 0)
					log.info("Processed Keywords: " + keywordId);
				Term term = tEnum.term();
				String field = term.field();
				String text = term.text();
				int keywordType = keywordTypes.get(field); 
				
				// For Test
				pw.print(keywordId + "\t" + field + ": " + text);
				pw.println();
				
				psInsertKeyw.setInt(1, keywordId);
				psInsertKeyw.setString(2, text);
				psInsertKeyw.setInt(3, keywordType);
				psInsertKeyw.executeUpdate();
				
				TermDocs tDocs = ireader.termDocs(term);
				while(tDocs.next()) {
					int docID = tDocs.doc();
					int termFreqInDoc = tDocs.freq();
					int docFreqOfTerm = ireader.docFreq(term);
					float score = 1f; 
					
					Document doc = ireader.document(docID, fieldSelector);
					int entityId = Integer.valueOf(doc.get(Environment.FIELD_ENTITY_ID)); 
					int conceptId = Integer.valueOf(doc.get(Environment.FIELD_CONCEPT_ID));
					int dsID = Integer.valueOf(doc.get(Environment.FIELD_DS_ID));
					
					psInsertKeywEntity.setInt(1, keywordId);
					psInsertKeywEntity.setInt(2, entityId);
					psInsertKeywEntity.setFloat(3, score);
					psInsertKeywEntity.setInt(4, keywordType);
					psInsertKeywEntity.setInt(5, conceptId);
					psInsertKeywEntity.setInt(6, dsID);
					psInsertKeywEntity.executeUpdate();
				}
			}
			
			ireader.close();
			directory.close();

			// For Test
			pw.close();

			long end = System.currentTimeMillis();
			log.info("Time for Creating Keyword Entity inclusion Table: " + (double) (end - start) / (double)1000  + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating keyword entity inclusion table:");
			log.warn(ex.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}
	
	public void createKeywordEntityInclusionTableUsingDB() {
		log.info("---- Creating Keyword Entity Inclusion Table and Keyword Table ----");
		long start = System.currentTimeMillis();
		HashSet<String> stopWords = loadStopWords();
		Stemmer stemmer = new Stemmer();
		Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        try {
			if (m_dbService.hasTable(Environment.KEYWORD_TABLE)) {
				stmt.execute("drop table " + Environment.KEYWORD_TABLE);
			}
			String createSql = "create table " + Environment.KEYWORD_TABLE + "( " + 
				Environment.KEYWORD_ID_COLUMN + " int unsigned not null primary key, " + 
				Environment.KEYWORD_COLUMN + " varchar(100) not null unique) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.KEYWORD_TABLE + " add index (" + Environment.KEYWORD_COLUMN + ")");
			
			if (m_dbService.hasTable(Environment.KEYWORD_ENTITY_INCLUSION_TABLE)) {
				stmt.execute("drop table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE);
			}
			createSql = "create table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + "( " + 
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " float unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_TYPE_COLUMN + " tinyint(1) unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_DS_ID_COLUMN + " smallint unsigned not null, " + 
				"primary key(" + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ", " +
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_TYPE_COLUMN + ", " +
				Environment.KEYWORD_ENTITY_INCLUSION_DS_ID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			
			log.info("---- Populating Keyword Entity Inclusion Table and Keyword Table ----");
			// Statement for Keyword Table
			String insertKeywSql = "insert into " + Environment.KEYWORD_TABLE + " values(?, ?)"; 
			PreparedStatement psInsertKeyw = m_dbService.createPreparedStatement(insertKeywSql, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
			String selectKeywTableSql = "select " + Environment.KEYWORD_ID_COLUMN +  
				" from " + Environment.KEYWORD_TABLE +
				" where " + Environment.KEYWORD_COLUMN + " = ?";
			PreparedStatement psQueryKeyw = m_dbService.createPreparedStatement(selectKeywTableSql);
			ResultSet rsKeyword = null;
			
			// Statement for Keyword Entity Inclusion Table
			String insertKeywEntitySql = "insert IGNORE into " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " values(?, ?, ?, ?, ?)"; 
			PreparedStatement psInsertKeywEntity = m_dbService.createPreparedStatement(insertKeywEntitySql, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
			
			// Statement for Entity Table
			String selectEntitySql = "select " + Environment.ENTITY_ID_COLUMN + ", " + Environment.ENTITY_DS_ID_COLUMN +
				" from " + Environment.ENTITY_TABLE +
				" where " + Environment.ENTITY_URI_COLUMN + " = ?";
			PreparedStatement psQueryEntity = m_dbService.createPreparedStatement(selectEntitySql);
			ResultSet rsEntity = null;
			
			// Statement for Triple Table
			String selectTripleSql = "select * from " + Environment.TRIPLE_TABLE;
			ResultSet rsTriple = stmt.executeQuery(selectTripleSql);
			
			int keywordSize = 0;
			// processing each triple
			int i = 0;
			while(rsTriple.next()) {
				i++;
				if(i % 1000 == 0)
					log.info(i);
				int propertyType = rsTriple.getInt(Environment.TRIPLE_PROPERTY_TYPE); 
				String subject = rsTriple.getString(Environment.TRIPLE_SUBJECT_COLUMN);
				String property = rsTriple.getString(Environment.TRIPLE_PROPERTY_COLUMN);
				String object = rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN);
				if(propertyType == Environment.DATA_PROPERTY) {
					psQueryEntity.setString(1, subject);
					rsEntity = psQueryEntity.executeQuery();
					rsEntity.next();
					int subjectId = rsEntity.getInt(Environment.ENTITY_ID_COLUMN);
					int dsId = rsEntity.getInt(Environment.ENTITY_DS_ID_COLUMN);
					rsEntity.close();
					
					// processing keyword for data property 
					String keywordOfDataProperty = trucateUri(property).toLowerCase();
					// stem the keyword
					stemmer.addWord(keywordOfDataProperty);
    				stemmer.stem();
    				keywordOfDataProperty = stemmer.toString();
					psQueryKeyw.setString(1, keywordOfDataProperty);
					rsKeyword = psQueryKeyw.executeQuery();
					int keywordId;
					if(rsKeyword.next()) {
						keywordId = rsKeyword.getInt(Environment.KEYWORD_ID_COLUMN);
					}
					else {
						keywordId = ++keywordSize;
						psInsertKeyw.setInt(1, keywordId);
						psInsertKeyw.setString(2, keywordOfDataProperty);
						psInsertKeyw.executeUpdate();
					}
					if(rsKeyword != null)
						rsKeyword.close();
					psInsertKeywEntity.setInt(1, keywordId);
					psInsertKeywEntity.setInt(2, subjectId);
					psInsertKeywEntity.setFloat(3, Environment.BOOST_KEYWORD_OF_DATA_PROPERTY);
					psInsertKeywEntity.setFloat(4, Environment.KEYWORD_OF_DATA_PROPERTY);
					psInsertKeywEntity.setInt(5, dsId);
					psInsertKeywEntity.executeUpdate();
					
					// processing keywords for data property value
					HashMap<String, Float> keywords = new HashMap<String, Float>();  
					KeywordTokenizer tokens = new KeywordTokenizer(object, stopWords);
					List<String> terms = tokens.getAllTerms();
					int termSize = terms.size();
					if(termSize <= 10) {
						for(String term : terms) {
							// stem the keyword
                            stemmer.addWord(term);
            				stemmer.stem();
            				term = stemmer.toString();
            				// count the number of occurrences of a keyword in data property value of an entity
            				if (keywords.containsKey(term)){
            					float value = keywords.get(term) + 1.0f;
            					keywords.put(term, value);
            				}
            				else
            					keywords.put(term, 1.0f);
						}
						for(String keyword : keywords.keySet()) {
							psQueryKeyw.setString(1, keyword);
							rsKeyword = psQueryKeyw.executeQuery();
							if(rsKeyword.next()) {
								keywordId = rsKeyword.getInt(Environment.KEYWORD_ID_COLUMN);
							}
							else {
								keywordId = ++keywordSize;
								psInsertKeyw.setInt(1, keywordId);
								psInsertKeyw.setString(2, keyword);
								psInsertKeyw.executeUpdate();
							}
							if(rsKeyword != null)
								rsKeyword.close();
							psInsertKeywEntity.setInt(1, keywordId);
							psInsertKeywEntity.setInt(2, subjectId);
							psInsertKeywEntity.setFloat(3, Environment.BOOST_KEYWORD_OF_DATA_VALUE*(keywords.get(keyword)/termSize));
							psInsertKeywEntity.setFloat(4, Environment.KEYWORD_OF_LITERAL);
							psInsertKeywEntity.setInt(5, dsId);
							psInsertKeywEntity.executeUpdate();
						}
					} 
					
				}
				else if(propertyType == Environment.OBJECT_PROPERTY) {
					psQueryEntity.setString(1, subject);
					rsEntity = psQueryEntity.executeQuery();
					rsEntity.next();
					int subjectId = rsEntity.getInt(Environment.ENTITY_ID_COLUMN);
					int dsId = rsEntity.getInt(Environment.ENTITY_DS_ID_COLUMN);
					rsEntity.close();
					
					psQueryEntity.setString(1, object);
					rsEntity = psQueryEntity.executeQuery();
					rsEntity.next();
					int objectId = rsEntity.getInt(Environment.ENTITY_ID_COLUMN);
					rsEntity.close();
					
					// processing keyword for object property 
					String keywordOfObjectProperty = trucateUri(property).toLowerCase();
					// stem the keyword
					stemmer.addWord(keywordOfObjectProperty);
    				stemmer.stem();
    				keywordOfObjectProperty = stemmer.toString();
					psQueryKeyw.setString(1, keywordOfObjectProperty);
					rsKeyword = psQueryKeyw.executeQuery();
					int keywordId;
					if(rsKeyword.next()) {
						keywordId = rsKeyword.getInt(Environment.KEYWORD_ID_COLUMN);
					}
					else {
						keywordId = ++keywordSize;
						psInsertKeyw.setInt(1, keywordId);
						psInsertKeyw.setString(2, keywordOfObjectProperty);
						psInsertKeyw.executeUpdate();
					}
					if(rsKeyword != null)
						rsKeyword.close();
					psInsertKeywEntity.setInt(1, keywordId);
					psInsertKeywEntity.setInt(2, subjectId);
					psInsertKeywEntity.setFloat(3, Environment.BOOST_KEYWORD_OF_OBJECT_PROPERTY);
					psInsertKeywEntity.setFloat(4, Environment.KEYWORD_OF_OBJECT_PROPERTY);
					psInsertKeywEntity.setInt(5, dsId);
					psInsertKeywEntity.executeUpdate();
					
					psInsertKeywEntity.setInt(1, keywordId);
					psInsertKeywEntity.setInt(2, objectId);
					psInsertKeywEntity.setFloat(3, Environment.BOOST_KEYWORD_OF_OBJECT_PROPERTY);
					psInsertKeywEntity.setFloat(4, Environment.KEYWORD_OF_OBJECT_PROPERTY);
					psInsertKeywEntity.setInt(5, dsId);
					psInsertKeywEntity.executeUpdate();
				}
				else if(propertyType == Environment.ENTITY_MEMBERSHIP_PROPERTY) {
					psQueryEntity.setString(1, subject);
					rsEntity = psQueryEntity.executeQuery();
					rsEntity.next();
					int subjectId = rsEntity.getInt(Environment.ENTITY_ID_COLUMN);
					int dsId = rsEntity.getInt(Environment.ENTITY_DS_ID_COLUMN);
					rsEntity.close();
					
					// processing keyword for concept 
					String keywordOfConcept = trucateUri(object).toLowerCase();
					// stem the keyword
					stemmer.addWord(keywordOfConcept);
    				stemmer.stem();
    				keywordOfConcept = stemmer.toString();
					psQueryKeyw.setString(1, keywordOfConcept);
					rsKeyword = psQueryKeyw.executeQuery();
					int keywordId;
					if(rsKeyword.next()) {
						keywordId = rsKeyword.getInt(Environment.KEYWORD_ID_COLUMN);
					}
					else {
						keywordId = ++keywordSize;
						psInsertKeyw.setInt(1, keywordId);
						psInsertKeyw.setString(2, keywordOfConcept);
						psInsertKeyw.executeUpdate();
					}
					if(rsKeyword != null)
						rsKeyword.close();
					psInsertKeywEntity.setInt(1, keywordId);
					psInsertKeywEntity.setInt(2, subjectId);
					psInsertKeywEntity.setFloat(3, Environment.BOOST_KEYWORD_OF_CONCEPT);
					psInsertKeywEntity.setFloat(4, Environment.KEYWORD_OF_CONCEPT);
					psInsertKeywEntity.setInt(5, dsId);
					psInsertKeywEntity.executeUpdate();
				}
			}
			
			if(rsEntity != null)
				rsEntity.close();
			if(rsKeyword != null)
				rsKeyword.close();
			if(psInsertKeyw != null)
				psInsertKeyw.close();
			if(psInsertKeywEntity != null)
				psInsertKeywEntity.close();
			if(psQueryKeyw != null)
				psQueryKeyw.close();
			if(psQueryEntity != null)
				psQueryEntity.close();
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Keyword Entity inclusion Table: " + (double)(end - start)/(double)60000 + "(min)");
			
			System.gc();
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating keyword entity inclusion table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createKeywordEntityReachabilityTable() {
		
	} 

	private HashSet<String> loadStopWords() {
		log.debug("---- Loading stopwords ----");
		HashSet<String> stopwords = new HashSet<String>(800);
		try {
			BufferedReader br = new BufferedReader(new FileReader(m_config.getStopwordFilePath()));
			String line;
			while ((line = br.readLine()) != null) {
				stopwords.add(line.toLowerCase());
			}
			br.close();
		} catch (Exception ex) {
			log.warn("Exception in readStopWords method:");
			log.warn(ex.getMessage());
		}
		return stopwords;
	}
	
	public static String trucateUri(String uri) {
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
	
	public void printMemoryInfo(){
		System.out.println("Total Memory: " + Runtime.getRuntime().totalMemory());
		System.out.println("Available Memory: " + Runtime.getRuntime().freeMemory());
	}
	
	class DbTripleSink implements TripleSink {
		PreparedStatement ps;
		
		public DbTripleSink() {
			String insertSql = "insert into " + Environment.TRIPLE_TABLE + "(" + 
				Environment.TRIPLE_SUBJECT_COLUMN +"," + 
				Environment.TRIPLE_PROPERTY_COLUMN +"," + 
				Environment.TRIPLE_OBJECT_COLUMN +"," + 
				Environment.TRIPLE_PROPERTY_TYPE +"," + 
				Environment.TRIPLE_DS_COLUMN +") values(?, ?, ?, ?, ?)";
			ps = m_dbService.createPreparedStatement(insertSql);
		}  
		
		public void triple(String subject, String property, String object, String ds, int type) {
			try {
				ps.setString(1, subject);
				ps.setString(2, property);
				ps.setString(3, object);
				ps.setInt(4, type);
				ps.setString(5, ds);
				ps.executeUpdate();
			} catch (SQLException ex) {
				log.warn("A warning in the process of importing triple into triple table:");
				log.warn(ex.getMessage());
			}
		}
		
		public void close() {
			try {
				if(ps != null)
					ps.close();
			} catch (SQLException ex) {
				log.warn("A warning in the process of closing prepared statement in DbTripleSink:");
				log.warn(ex.getMessage());
			}
		}
	}

}
