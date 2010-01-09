package edu.unika.aifb.ease.index;

import java.io.File;
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
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
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

import edu.unika.aifb.ease.Config;
import edu.unika.aifb.ease.Environment;
import edu.unika.aifb.ease.db.DBService;
import edu.unika.aifb.ease.importer.Importer;
import edu.unika.aifb.ease.importer.N3Importer;
import edu.unika.aifb.ease.importer.NxImporter;
import edu.unika.aifb.ease.importer.RDFImporter;
import edu.unika.aifb.ease.importer.TripleSink;

public class DBIndexService {
	
	private static final Logger log = Logger.getLogger(DBIndexService.class);
	
	private DBService m_dbService;
	private Map<Integer, Importer> m_importers;
	private Config m_config;
	
	
	public DBIndexService(Config config) {
		this(config, false);
	}
	
	public DBIndexService(Config config, boolean createDb) {
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
		m_dbService = new DBService(server, username, password, port, dbName, createDb);
	}
	
	public void close() {
		m_dbService.close();
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
            	} 
            	else {
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
					if(++num % 10000 == 0)
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
					if(++num % 10000 == 0)
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
					if(++num % 10000 == 0)
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
				
//				int deletedRows = 0;
//				for(int i=1; i<distance; i++){
//					String R_i = Environment.ENTITY_RELATION_TABLE + i;
//					String deleteSql = "delete " + R_d + " from " + R_d + ", " + R_i +
//						" where " + R_d + "." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
//						R_i + "." + Environment.ENTITY_RELATION_UID_COLUMN + " and " + 
//						R_d + "." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
//						R_i + "." + Environment.ENTITY_RELATION_VID_COLUMN;
//					deletedRows += stmt.executeUpdate(deleteSql);
//				}
//				log.info("Number of duplicated rows that are deleted: " + deletedRows);
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
					if(++num % 10000 == 0)
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
					if(++num % 10000 == 0)
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
					if(++num % 10000 == 0)
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
					if(++num % 10000 == 0)
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
	
//				int deletedRows = 0;
//				for(int i=1; i<distance; i++){
//					String R_i = Environment.ENTITY_RELATION_TABLE + i;
//					String deleteSql = "delete " + R_d + " from " + R_d + ", " + R_i +
//						" where " + R_d + "." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
//						R_i + "." + Environment.ENTITY_RELATION_UID_COLUMN + " and " + 
//						R_d + "." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
//						R_i + "." + Environment.ENTITY_RELATION_VID_COLUMN;
//					deletedRows += stmt.executeUpdate(deleteSql);
//				}
//				log.info("Number of duplicated rows that are deleted: " + deletedRows);
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
	
	public void createRRadiusGraphCenterTable() {
		log.info("---- Creating r-Radius Graph Center Table ----");
		long start = System.currentTimeMillis();
		
		String R_max_d = Environment.ENTITY_RELATION_TABLE + m_config.getMaxRadius(); 
		Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        try {
        	// Create r-Radius Graph Center Table 
			if (m_dbService.hasTable(Environment.R_RADIUS_GRAPH_CENTER_TABLE)) {
				stmt.execute("drop table " + Environment.R_RADIUS_GRAPH_CENTER_TABLE);
			}
			String createSql = "create table " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + "( " + 
				Environment.GRAPH_CENTER_ID_COLUMN + " int unsigned not null primary key, " + 
				Environment.GRAPH_CENTER_URI_COLUMN + " varchar(100) not null, " +
				Environment.GRAPH_SIZE_COLUMN + " mediumint unsigned not null default 1, " +
				Environment.GRAPH_IS_MAX_COLUMN + " tinyint not null default " + Environment.IS_MAX_GRAPH_UNKOWN + ") " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + " add index (" + Environment.GRAPH_CENTER_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + " add index (" + Environment.GRAPH_SIZE_COLUMN + ")");
			stmt.execute("alter table " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + " add index (" + Environment.GRAPH_IS_MAX_COLUMN + ")");
			
			log.info("---- Populating r-Radius Graph Table ----");
			// Populate r-Radius Graph Center Table 
			String insertSql = "insert IGNORE into " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + 
				"(" + Environment.GRAPH_CENTER_ID_COLUMN + ", " + Environment.GRAPH_CENTER_URI_COLUMN + ") "; 
			String selectSql = "select " + Environment.ENTITY_ID_COLUMN + ", " + Environment.ENTITY_URI_COLUMN + 
				" from " + Environment.ENTITY_TABLE; 
			stmt.executeUpdate(insertSql + selectSql);
			
			String updateSql = "update " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + 
				" set " + Environment.GRAPH_SIZE_COLUMN + " = " + Environment.GRAPH_SIZE_COLUMN + " + ? " + 
				" where " + Environment.GRAPH_CENTER_ID_COLUMN + " = " + "?";
			PreparedStatement ps = m_dbService.createPreparedStatement(updateSql);
			
			selectSql = "select " + Environment.ENTITY_RELATION_UID_COLUMN + ", count(*) " + 
				" from " + R_max_d + 
				" group by " + Environment.ENTITY_RELATION_UID_COLUMN;  
			ResultSet rs = stmt.executeQuery(selectSql);
			while (rs.next()) {
				int entityId = rs.getInt(1);
				int size = rs.getInt(2) + 1;
				ps.setInt(1, size);
				ps.setInt(2, entityId);
				ps.executeUpdate();
			}
			if(rs != null)
				rs.close();
				
			selectSql = "select " + Environment.ENTITY_RELATION_VID_COLUMN + ", count(*) " + 
				" from " + R_max_d + 
				" group by " + Environment.ENTITY_RELATION_VID_COLUMN;  
			rs = stmt.executeQuery(selectSql);
			while (rs.next()) {
				int entityId = rs.getInt(1);
				int size = rs.getInt(2);
				ps.setInt(1, size);
				ps.setInt(2, entityId);
				ps.executeUpdate();
			}
			if(rs != null)
				rs.close();
			if(ps != null)
				ps.close();
			
			if(stmt != null)
            	stmt.close();
			
            long end = System.currentTimeMillis();
			log.info("Time for Creating r-Radius Graph Center Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating r-Radius graph center table:");
			log.warn(ex.getMessage());
		}  
	}
	
	public void findMaxRRadiusGraphCenter() {
		long start = System.currentTimeMillis();
		
		String R_max_d = Environment.ENTITY_RELATION_TABLE + m_config.getMaxRadius(); 
		Statement stmt = m_dbService.createStatement();
        try {
			log.info("---- Finding Max r-Radius Graphs ----");
			// Find the max r-Radius Graph Center
			String updateSql = "update " + Environment.R_RADIUS_GRAPH_CENTER_TABLE +
				" set " + Environment.GRAPH_IS_MAX_COLUMN + " = " + Environment.IS_MAX_GRAPH + 
				" where " + Environment.GRAPH_SIZE_COLUMN + " >= " + m_config.getSizeOfGraphWithoutCheck();
			int numCenters = stmt.executeUpdate(updateSql);
			log.info("Processed Centers: " + numCenters + "\t" + "Processed Graph Size: " + m_config.getSizeOfGraphWithoutCheck());
			
			String selectCenterSql = "select " + Environment.GRAPH_CENTER_ID_COLUMN + ", " + Environment.GRAPH_SIZE_COLUMN +
				" from " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + 
				" where " + Environment.GRAPH_IS_MAX_COLUMN + " = " + Environment.IS_MAX_GRAPH_UNKOWN + 
				" order by " + Environment.GRAPH_SIZE_COLUMN + " desc " + 
				" limit 1";
			
			String selectNeighborhoodSql = "select distinct " + Environment.ENTITY_RELATION_VID_COLUMN + 
				" from " + R_max_d + 
				" where " + Environment.ENTITY_RELATION_UID_COLUMN + " = " + "?"+ 
				" union distinct " +  "select distinct " + Environment.ENTITY_RELATION_UID_COLUMN + 
				" from " + R_max_d + 
				" where " + Environment.ENTITY_RELATION_VID_COLUMN + " = " + "?"; 
			PreparedStatement psSelectNeighborhood = m_dbService.createPreparedStatement(selectNeighborhoodSql);
			
			updateSql = "update " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + 
				" set " + Environment.GRAPH_IS_MAX_COLUMN + " = " + "?" + 
				" where " + Environment.GRAPH_CENTER_ID_COLUMN + " = " + "?";
			PreparedStatement psUpdate = m_dbService.createPreparedStatement(updateSql);	
			
			ResultSet rs = stmt.executeQuery(selectCenterSql);
			while(rs.next()) {
				int centerId1 = rs.getInt(1);
				int size = rs.getInt(2);
				rs.close();
				psSelectNeighborhood.setInt(1, centerId1);
				psSelectNeighborhood.setInt(2, centerId1);
				rs = psSelectNeighborhood.executeQuery();
				Set<Integer> neighbors1 = new HashSet<Integer>();
				neighbors1.add(centerId1);
				while(rs.next()) 
					neighbors1.add(rs.getInt(1));
				rs.close();
				
				boolean isMaxGraph = true;
				for(int centerId2 : neighbors1) {
					if(centerId2 == centerId1)
						continue;
					psSelectNeighborhood.setInt(1, centerId2);
					psSelectNeighborhood.setInt(2, centerId2);
					rs = psSelectNeighborhood.executeQuery();
					Set<Integer> neighbors2 = new HashSet<Integer>();
					neighbors2.add(centerId2);
					while(rs.next()) 
						neighbors2.add(rs.getInt(1));
					rs.close();
					if(neighbors1.containsAll(neighbors2)) {
						psUpdate.setInt(1, Environment.IS_NOT_MAX_GRAPH);
						psUpdate.setInt(2, centerId2);
						psUpdate.executeUpdate();
					}
					else if(isMaxGraph != false && neighbors2.containsAll(neighbors1)) {
						isMaxGraph = false;
					}
				}
				if(isMaxGraph) {
					psUpdate.setInt(1, Environment.IS_MAX_GRAPH);
					psUpdate.setInt(2, centerId1);
					psUpdate.executeUpdate();
				}
				else {
					psUpdate.setInt(1, Environment.IS_NOT_MAX_GRAPH);
					psUpdate.setInt(2, centerId1);
					psUpdate.executeUpdate();
				}
				
				log.info("Processed Centers: " + ++numCenters + "\t" + "Processed Graph Size: " + size);
				rs = stmt.executeQuery(selectCenterSql);
			}
			
			if(psSelectNeighborhood != null)
				psSelectNeighborhood.close();
			if(psUpdate != null)
				psUpdate.close();
			
			if(stmt != null)
            	stmt.close();
            
            long end = System.currentTimeMillis();
			log.info("Time for Finding Max r-Radius Graphs: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of finding max r-Radius Graph table:");
			log.warn(ex.getMessage());
		}  
	}
	
	public void createMaxRRadiusGraphTable() {
		log.info("---- Creating Max r-Radius Graph Table ----");
		long start = System.currentTimeMillis();
		
		String R_max_d = Environment.ENTITY_RELATION_TABLE + m_config.getMaxRadius(); 
		Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        try {
        	// Create Max r-Radius Graph Table
			if (m_dbService.hasTable(Environment.MAX_R_RADIUS_GRAPH_TABLE)) {
				stmt.execute("drop table " + Environment.MAX_R_RADIUS_GRAPH_TABLE);
			}
			String createSql = "create table " + Environment.MAX_R_RADIUS_GRAPH_TABLE + "( " + 
				Environment.MAX_GRAPH_CENTER_ID_COLUMN + " int unsigned not null, " + 
				Environment.MAX_GRAPH_VERTEX_ID_COLUMN + " int unsigned not null, " + 
				"primary key(" + Environment.MAX_GRAPH_CENTER_ID_COLUMN + ", " + Environment.MAX_GRAPH_VERTEX_ID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.MAX_R_RADIUS_GRAPH_TABLE + " add index (" + Environment.MAX_GRAPH_CENTER_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.MAX_R_RADIUS_GRAPH_TABLE + " add index (" + Environment.MAX_GRAPH_VERTEX_ID_COLUMN + ")");
			
			log.info("---- Populating Max r-Radius Graph Table ----");
			// Populate Max r-Radius Graph Table
			String insertSql = "insert IGNORE into " + Environment.MAX_R_RADIUS_GRAPH_TABLE + 
				"(" + Environment.MAX_GRAPH_CENTER_ID_COLUMN + ", " + Environment.MAX_GRAPH_VERTEX_ID_COLUMN + ") "; 
			String selectSql = "select distinct " + "B." + Environment.GRAPH_CENTER_ID_COLUMN + ", A." + Environment.ENTITY_RELATION_VID_COLUMN + 
				" from " + R_max_d + " as A, " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + " as B " +
				" where " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + "B." + Environment.GRAPH_CENTER_ID_COLUMN +
				" and " + "B." + Environment.GRAPH_IS_MAX_COLUMN + " = " + Environment.IS_MAX_GRAPH;
			stmt.executeUpdate(insertSql + selectSql);
			
			selectSql = "select distinct " + "B." + Environment.GRAPH_CENTER_ID_COLUMN + ", A." + Environment.ENTITY_RELATION_UID_COLUMN + 
				" from " + R_max_d + " as A, " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + " as B " +
				" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + "B." + Environment.GRAPH_CENTER_ID_COLUMN +
				" and " + "B." + Environment.GRAPH_IS_MAX_COLUMN + " = " + Environment.IS_MAX_GRAPH; 
			stmt.executeUpdate(insertSql + selectSql);
			
			selectSql = "select " + Environment.GRAPH_CENTER_ID_COLUMN + ", " + Environment.GRAPH_CENTER_ID_COLUMN +
				" from " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + 
				" where " + Environment.GRAPH_IS_MAX_COLUMN + " = " + Environment.IS_MAX_GRAPH; 
			stmt.executeUpdate(insertSql + selectSql);
			
			if(stmt != null)
            	stmt.close();
			
            long end = System.currentTimeMillis();
			log.info("Time for Creating Max r-Radius Graph Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating max r-Radius graph table:");
			log.warn(ex.getMessage());
		}  
	}
	
	public void createKeywordEntityLuceneIndex() {
		log.info("---- Creating Keyword Index ----");
		long start = System.currentTimeMillis();
		
		String indexPath = m_config.getTemporaryDirectory() + "/entity";
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
			String selectEntitySql = "select " + Environment.ENTITY_ID_COLUMN + ", " + Environment.ENTITY_URI_COLUMN + 
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
				if(numEntities % 10000 == 0)
					log.info("Processed Entities: " + numEntities);
				String entityUri = rsEntity.getString(Environment.ENTITY_URI_COLUMN);
				int nEntityId = rsEntity.getInt(Environment.ENTITY_ID_COLUMN);
				String entityId = String.valueOf(nEntityId);
				String termsOfLiterals = trucateUri(entityUri) + " ";
				String termsOfDataProperties = "";
				String termsOfObjectProperties = "";
				String termsOfConcepts = "";
				
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
						termsOfLiterals += rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN) + " ";
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
					doc.add(new Field(Environment.FIELD_ENTITY_TERM_LITERAL, termsOfLiterals, Field.Store.NO, Field.Index.ANALYZED));
				if(!termsOfConcepts.equals(""))
					doc.add(new Field(Environment.FIELD_ENTITY_TERM_CONCEPT, termsOfConcepts, Field.Store.NO, Field.Index.ANALYZED));
				if(!termsOfDataProperties.equals(""))
					doc.add(new Field(Environment.FIELD_ENTITY_TERM_DATAPROPERTY, termsOfDataProperties, Field.Store.NO, Field.Index.ANALYZED));
				if(!termsOfObjectProperties.equals(""))
					doc.add(new Field(Environment.FIELD_ENTITY_TERM_OBJECTPROPERTY, termsOfObjectProperties, Field.Store.NO, Field.Index.ANALYZED));
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
	
	public void createKeywordEntityInclusionTable() {
		// construct entity keyword index 
		createKeywordEntityLuceneIndex();
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
				Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " double unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_TYPE_COLUMN + " tinyint(1) unsigned not null, " + 
				"primary key(" + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + 
					" add index (" + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + 
					" add index (" + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ")");
			
			log.info("---- Populating Keyword Entity Inclusion Table and Keyword Table ----");
			// Statement for Keyword Table
			String insertKeywSql = "insert into " + Environment.KEYWORD_TABLE + " values(?, ?, ?)"; 
			PreparedStatement psInsertKeyw = m_dbService.createPreparedStatement(insertKeywSql);
			
			// Statement for Keyword Entity Inclusion Table
			String insertKeywEntitySql = "insert IGNORE into " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " values(?, ?, ?, ?)"; 
			PreparedStatement psInsertKeywEntity = m_dbService.createPreparedStatement(insertKeywEntitySql);
			
			// Retrieve Keywords from Lucene Index
			Directory directory = FSDirectory.open(new File(m_config.getTemporaryDirectory() + "/entity"));
			IndexReader ireader = IndexReader.open(directory, true);
			
			int numDocs = ireader.numDocs();
			
			String[] loadFields = {Environment.FIELD_ENTITY_ID};
			MapFieldSelector fieldSelector = new MapFieldSelector(loadFields);
			Map<String,Integer> keywordTypes = new HashMap<String,Integer>();
			keywordTypes.put(Environment.FIELD_ENTITY_TERM_LITERAL, Environment.KEYWORD_OF_LITERAL);
			keywordTypes.put(Environment.FIELD_ENTITY_TERM_DATAPROPERTY, Environment.KEYWORD_OF_DATA_PROPERTY);
			keywordTypes.put(Environment.FIELD_ENTITY_TERM_OBJECTPROPERTY, Environment.KEYWORD_OF_OBJECT_PROPERTY);
			keywordTypes.put(Environment.FIELD_ENTITY_TERM_CONCEPT, Environment.KEYWORD_OF_CONCEPT);
			
			// For Test
//			PrintWriter pw = new PrintWriter("./res/keyword.txt"); 
			
			int keywordId = 0;
			TermEnum tEnum = ireader.terms();
			while(tEnum.next()) {
				keywordId++;
				if(keywordId % 10000 == 0)
					log.info("Processed Keywords: " + keywordId);
				Term term = tEnum.term();
				String field = term.field();
				String text = term.text();
				int keywordType = keywordTypes.get(field); 
				
				// For Test
//				pw.print(keywordId + "\t" + field + ": " + text);
//				pw.println();
				
				psInsertKeyw.setInt(1, keywordId);
				psInsertKeyw.setString(2, text);
				psInsertKeyw.setInt(3, keywordType);
				psInsertKeyw.executeUpdate();
				
				TermDocs tDocs = ireader.termDocs(term);
				while(tDocs.next()) {
					int docID = tDocs.doc();
					int termFreqInDoc = tDocs.freq();
					int docFreqOfTerm = ireader.docFreq(term);
					double score = (1 + Math.log(1 + Math.log(termFreqInDoc)))*Math.log((numDocs + 1)/docFreqOfTerm);
					
					Document doc = ireader.document(docID, fieldSelector);
					int entityId = Integer.valueOf(doc.get(Environment.FIELD_ENTITY_ID)); 
					
					psInsertKeywEntity.setInt(1, keywordId);
					psInsertKeywEntity.setInt(2, entityId);
					psInsertKeywEntity.setDouble(3, score);
					psInsertKeywEntity.setInt(4, keywordType);
					psInsertKeywEntity.executeUpdate();
				}
			}
			
			ireader.close();
			directory.close();

			// For Test
//			pw.close();

			long end = System.currentTimeMillis();
			log.info("Time for Creating Keyword Entity inclusion Table and Keyword Table: " + (double) (end - start) / (double)1000  + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating keyword entity inclusion table and keyword table:");
			log.warn(ex.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}
	
	public void createKeywordGraphInclusionTable() {
		log.info("---- Creating Keyword Graph Inclusion Table ----");
		long start = System.currentTimeMillis();
		
		Statement stmt = m_dbService.createStatement();
		try {
			if (m_dbService.hasTable(Environment.KEYWORD_GRAPH_INCLUSION_TABLE)) {
				stmt.execute("drop table " + Environment.KEYWORD_GRAPH_INCLUSION_TABLE);
			}
			String createSql = "create table " + Environment.KEYWORD_GRAPH_INCLUSION_TABLE + "( " + 
				Environment.KEYWORD_GRAPH_INCLUSION_KEYWORD_ID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_GRAPH_INCLUSION_CENTER_ID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_GRAPH_INCLUSION_SCORE_COLUMN + " double unsigned not null, " + 
				Environment.KEYWORD_GRAPH_INCLUSION_KEYWORD_TYPE_COLUMN + " tinyint(1) unsigned not null, " + 
				"primary key(" + Environment.KEYWORD_GRAPH_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				Environment.KEYWORD_GRAPH_INCLUSION_CENTER_ID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.KEYWORD_GRAPH_INCLUSION_TABLE + 
					" add index (" + Environment.KEYWORD_GRAPH_INCLUSION_KEYWORD_ID_COLUMN + ")");
			
			log.info("---- Populating Keyword Graph Inclusion Table ----");
			// Statement for Keyword Graph Inclusion Table
			String insertSql = "insert IGNORE into " + Environment.KEYWORD_GRAPH_INCLUSION_TABLE + " "; 
			String selectSql = "select " + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				Environment.MAX_GRAPH_CENTER_ID_COLUMN + ", " + 
				"sum(" + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ")" + ", " + 
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_TYPE_COLUMN + 
				" from " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + ", " + Environment.MAX_R_RADIUS_GRAPH_TABLE + 
				" where " + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + Environment.MAX_GRAPH_VERTEX_ID_COLUMN + 
				" group by " + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + Environment.MAX_GRAPH_CENTER_ID_COLUMN; 
			stmt.executeUpdate(insertSql + selectSql);

			long end = System.currentTimeMillis();
			log.info("Time for Creating Keyword Graph inclusion Table: " + (double) (end - start) / (double)1000  + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating keyword graph inclusion table:");
			log.warn(ex.getMessage());
		} 
	}
	
	public void createKeywordGraphLuceneIndex() {
		log.info("---- Creating Keyword Index ----");
		long start = System.currentTimeMillis();
		
		String indexPathGraph = m_config.getTemporaryDirectory() + "/graph";
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
//		Analyzer analyzer = new WhitespaceAnalyzer();
		// Store the index on disk:
		File dir = new File(indexPathGraph);
		if(!dir.exists())
			dir.mkdirs();
		
		// Retrieve Keywords from Lucene Index
		
		String R_max_d = Environment.ENTITY_RELATION_TABLE + m_config.getMaxRadius(); 
        Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
		try {
			Directory readDirectory = FSDirectory.open(new File(m_config.getTemporaryDirectory() + "/entity"));
			IndexReader ireader = IndexReader.open(readDirectory, true);
			
			Directory indexDirectory = FSDirectory.open(dir);
			IndexWriter iwriter = new IndexWriter(indexDirectory, analyzer, true, new IndexWriter.MaxFieldLength(25000));
			
			// Statement for r-Radius Graph Table
			String selectCenterSql = "select " + Environment.GRAPH_CENTER_ID_COLUMN + ", " + Environment.GRAPH_CENTER_URI_COLUMN +  
				" from " + Environment.R_RADIUS_GRAPH_CENTER_TABLE + 
				" where " + Environment.GRAPH_IS_MAX_COLUMN + " = " + Environment.IS_MAX_GRAPH;
			ResultSet rsCenter = stmt.executeQuery(selectCenterSql);
			
			// Statement for Entity Relationship Table
			String selectNeighborhoodSql = "select distinct " + Environment.ENTITY_RELATION_VID_COLUMN + 
				" from " + R_max_d + 
				" where " + Environment.ENTITY_RELATION_UID_COLUMN + " = " + "?"+ 
				" union distinct " +  "select distinct " + Environment.ENTITY_RELATION_UID_COLUMN + 
				" from " + R_max_d + 
				" where " + Environment.ENTITY_RELATION_VID_COLUMN + " = " + "?"; 
			PreparedStatement psSelectNeighborhood = m_dbService.createPreparedStatement(selectNeighborhoodSql);
			ResultSet rsNeighbors = null;
			
			int numCenters = 0;
			// processing each center of the max r-Radius graphs
			while(rsCenter.next()) {
				numCenters++;
				if(numCenters % 10 == 0)
					log.info("Processed Centers of Max r-Radius Graph: " + numCenters);
				String centerUri = rsCenter.getString(Environment.GRAPH_CENTER_URI_COLUMN);
				int nCenterId = rsCenter.getInt(Environment.GRAPH_CENTER_ID_COLUMN);
				String centerId = String.valueOf(nCenterId);
				String termsOfLiterals = "";
				String termsOfDataProperties = "";
				String termsOfObjectProperties = "";
				String termsOfConcepts = "";
				
				// processing keywords of all vertices in the max r-Radius graphs 
				psSelectNeighborhood.setInt(1, nCenterId);
				psSelectNeighborhood.setInt(2, nCenterId);
				rsNeighbors = psSelectNeighborhood.executeQuery();
				Set<Integer> neighbors = new HashSet<Integer>();
				neighbors.add(nCenterId);
				while(rsNeighbors.next()) 
					neighbors.add(rsNeighbors.getInt(1));
				rsNeighbors.close();
				
				for(int nVertexId : neighbors) {
					String vertexId =  String.valueOf(nVertexId);
					Term term = new Term(Environment.FIELD_CENTER_ID, vertexId);
					TermDocs tDocs = ireader.termDocs(term);
					if(tDocs.next()) {
						int docID = tDocs.doc();
						Document doc = ireader.document(docID);
						termsOfLiterals += doc.get(Environment.FIELD_ENTITY_TERM_LITERAL);
						termsOfDataProperties += doc.get(Environment.FIELD_ENTITY_TERM_DATAPROPERTY);
						termsOfObjectProperties += doc.get(Environment.FIELD_ENTITY_TERM_OBJECTPROPERTY);
						termsOfConcepts += doc.get(Environment.FIELD_ENTITY_TERM_CONCEPT);
					}
				}
				
				Document doc = new Document();
				doc.add(new Field(Environment.FIELD_CENTER_URI, centerUri, Field.Store.YES, Field.Index.NO));
				doc.add(new Field(Environment.FIELD_CENTER_ID, centerId, Field.Store.YES, Field.Index.NO));
				if(!termsOfLiterals.equals(""))
					doc.add(new Field(Environment.FIELD_GRAPH_TERM_LITERAL, termsOfLiterals, Field.Store.NO, Field.Index.ANALYZED));
				if(!termsOfConcepts.equals(""))
					doc.add(new Field(Environment.FIELD_GRAPH_TERM_CONCEPT, termsOfConcepts, Field.Store.NO, Field.Index.ANALYZED));
				if(!termsOfDataProperties.equals(""))
					doc.add(new Field(Environment.FIELD_GRAPH_TERM_DATAPROPERTY, termsOfDataProperties, Field.Store.NO, Field.Index.ANALYZED));
				if(!termsOfObjectProperties.equals(""))
					doc.add(new Field(Environment.FIELD_GRAPH_TERM_OBJECTPROPERTY, termsOfObjectProperties, Field.Store.NO, Field.Index.ANALYZED));
				iwriter.addDocument(doc);
			}	
			
			if(rsCenter != null)
				rsCenter.close();
			if(psSelectNeighborhood != null)
				psSelectNeighborhood.close();
			if(stmt != null)
				stmt.close();
			
			ireader.close();
			readDirectory.close();
			iwriter.close();
			indexDirectory.close();
			
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
	
//	public void createKeywordGraphLuceneIndex() {
//		log.info("---- Creating Keyword Index ----");
//		long start = System.currentTimeMillis();
//		
//		String indexPath = m_config.getTemporaryDirectory() + "/graph";
//		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
////		Analyzer analyzer = new WhitespaceAnalyzer();
//		// Store the index on disk:
//		File dir = new File(indexPath);
//		if(!dir.exists())
//			dir.mkdirs();
//		Directory directory;
//		
//		String R_max_d = Environment.ENTITY_RELATION_TABLE + m_config.getMaxDistance(); 
//        Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
//		try {
//			directory = FSDirectory.open(dir);
//			IndexWriter iwriter = new IndexWriter(directory, analyzer, true, new IndexWriter.MaxFieldLength(25000));
//			
//			// Statement for r-Radius Graph Table
//			String selectCenterSql = "select " + Environment.GRAPH_CENTER_ENTITY_ID_COLUMN + ", " + Environment.GRAPH_CENTER_ENTITY_URI_COLUMN +  
//				" from " + Environment.R_RADIUS_GRAPH_TABLE + 
//				" where " + Environment.GRAPH_IS_MAX_COLUMN + " = " + Environment.IS_MAX_GRAPH;
//			ResultSet rsCenter = stmt.executeQuery(selectCenterSql);
//			
//			// Statement for Triple Table
//			String selectTripleSqlFw = "select distinct " + " B." + Environment.TRIPLE_PROPERTY_TYPE + ", " + 
//				" B." + Environment.TRIPLE_PROPERTY_COLUMN + ", " + " B." + Environment.TRIPLE_OBJECT_COLUMN +
//				" from " + R_max_d + " as A, " + Environment.TRIPLE_TABLE + " as B " +
//				" where " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + "?" + 
//				" and " + "B." + Environment.TRIPLE_SUBJECT_ID_COLUMN + " = " + "A." + Environment.ENTITY_RELATION_VID_COLUMN +
//				" union distinct " +  "select distinct " + " B." + Environment.TRIPLE_PROPERTY_TYPE + ", " + 
//				" B." + Environment.TRIPLE_PROPERTY_COLUMN + ", " + " B." + Environment.TRIPLE_OBJECT_COLUMN +
//				" from " + R_max_d + " as A, " + Environment.TRIPLE_TABLE + " as B " +
//				" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + "?" + 
//				" and " + "B." + Environment.TRIPLE_SUBJECT_ID_COLUMN + " = " + "A." + Environment.ENTITY_RELATION_UID_COLUMN +
//				" union distinct " +  "select distinct " + Environment.TRIPLE_PROPERTY_TYPE + ", " + 
//				Environment.TRIPLE_PROPERTY_COLUMN + ", " + Environment.TRIPLE_OBJECT_COLUMN + 
//				" from " + Environment.TRIPLE_TABLE +
//				" where " + Environment.TRIPLE_SUBJECT_ID_COLUMN + " = ?";
//			PreparedStatement psQueryTripleFw = m_dbService.createPreparedStatement(selectTripleSqlFw);
//			ResultSet rsTriple = null;
//			
//			int numCenters = 0;
//			// processing each center of the max r-Radius graphs
//			while(rsCenter.next()) {
//				numCenters++;
//				if(numCenters % 10 == 0)
//					log.info("Processed Centers of Max r-Radius Graph: " + numCenters);
//				String centerUri = rsCenter.getString(Environment.GRAPH_CENTER_ENTITY_URI_COLUMN);
//				int nCenterId = rsCenter.getInt(Environment.GRAPH_CENTER_ENTITY_ID_COLUMN);
//				String centerId = String.valueOf(nCenterId);
//				String termsOfLiterals = "";
//				String termsOfDataProperties = "";
//				String termsOfObjectProperties = "";
//				String termsOfConcepts = "";
//				
//				// processing keywords of all vertices in the max r-Radius graphs 
//				psQueryTripleFw.setInt(1, nCenterId);
//				psQueryTripleFw.setInt(2, nCenterId);
//				psQueryTripleFw.setInt(3, nCenterId);
//				rsTriple = psQueryTripleFw.executeQuery();
//				while (rsTriple.next()) {
//					int type = rsTriple.getInt(Environment.TRIPLE_PROPERTY_TYPE);
//					if(type == Environment.DATA_PROPERTY) {
//						// term for data property 
//						String dataProperty = rsTriple.getString(Environment.TRIPLE_PROPERTY_COLUMN);
//						if(!dataProperty.startsWith(RDF.NAMESPACE) && !dataProperty.startsWith(RDFS.NAMESPACE)) {
//							termsOfDataProperties += trucateUri(dataProperty) + " ";
//						}
//						// term for literal
//						termsOfLiterals += rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN) + " ";
//					}
//					else if(type == Environment.OBJECT_PROPERTY) {
//						// term for object property 
//						String objectProperty = rsTriple.getString(Environment.TRIPLE_PROPERTY_COLUMN); 
//						if(!objectProperty.startsWith(RDF.NAMESPACE) && !objectProperty.startsWith(RDFS.NAMESPACE)) {
//							termsOfObjectProperties += trucateUri(objectProperty) + " ";
//						}
//					}
//					else if(type == Environment.ENTITY_MEMBERSHIP_PROPERTY) {
//						// term for concept 
//						String concept = rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN);
//						if(!concept.startsWith(RDF.NAMESPACE) && !concept.startsWith(RDFS.NAMESPACE)) {
//							termsOfConcepts += trucateUri(concept) + " ";
//						}
//					}
//				}
//				if(rsTriple != null)
//					rsTriple.close();
//				
//				Document doc = new Document();
//				doc.add(new Field(Environment.FIELD_CENTER_URI, centerUri, Field.Store.YES, Field.Index.NO));
//				doc.add(new Field(Environment.FIELD_CENTER_ID, centerId, Field.Store.YES, Field.Index.NO));
//				if(!termsOfLiterals.equals(""))
//					doc.add(new Field(Environment.FIELD_GRAPH_TERM_LITERAL, termsOfLiterals, Field.Store.NO, Field.Index.ANALYZED));
//				if(!termsOfConcepts.equals(""))
//					doc.add(new Field(Environment.FIELD_GRAPH_TERM_CONCEPT, termsOfConcepts, Field.Store.NO, Field.Index.ANALYZED));
//				if(!termsOfDataProperties.equals(""))
//					doc.add(new Field(Environment.FIELD_GRAPH_TERM_DATAPROPERTY, termsOfDataProperties, Field.Store.NO, Field.Index.ANALYZED));
//				if(!termsOfObjectProperties.equals(""))
//					doc.add(new Field(Environment.FIELD_GRAPH_TERM_OBJECTPROPERTY, termsOfObjectProperties, Field.Store.NO, Field.Index.ANALYZED));
//				iwriter.addDocument(doc);
//			}	
//			
//			if(rsCenter != null)
//				rsCenter.close();
//			if(psQueryTripleFw != null)
//				psQueryTripleFw.close();
//			if(stmt != null)
//				stmt.close();
//			
//			iwriter.close();
//			directory.close();
//			
//			long end = System.currentTimeMillis();
//			log.info("Time for Creating Keyword Index: " + (double)(end - start)/(double)1000 + "(sec)");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	public void cleanDB() {
		
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
