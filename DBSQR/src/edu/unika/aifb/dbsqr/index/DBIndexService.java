package edu.unika.aifb.dbsqr.index;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.dbsqr.Config;
import edu.unika.aifb.dbsqr.Environment;
import edu.unika.aifb.dbsqr.db.DBService;
import edu.unika.aifb.dbsqr.importer.Importer;
import edu.unika.aifb.dbsqr.importer.N3Importer;
import edu.unika.aifb.dbsqr.importer.NxImporter;
import edu.unika.aifb.dbsqr.importer.RDFImporter;
import edu.unika.aifb.dbsqr.importer.TripleSink;
import edu.unika.aifb.dbsqr.util.Timing;

public class DBIndexService {
	
	private static final Logger log = Logger.getLogger(DBIndexService.class);
	
	private DBService m_dbService;
	private Map<Integer, Importer> m_importers;
	private Config m_config;
	private Timing m_timing;
	
	public DBIndexService(Config config) {
		this(config, false);
	}
	
	public DBIndexService(Config config, boolean createDb) {
		m_config = config;
		m_importers = new HashMap<Integer, Importer>();
		m_timing = new Timing(m_config.getTemporaryDirectory() + m_config.getDbName() + "/log"); 
		
		initializeDbService(createDb);	
	}
	
	public DBIndexService(Config config, Timing timing) {
		this(config, timing, false);
	}
	
	public DBIndexService(Config config, Timing timing, boolean createDb) {
		m_config = config;
		m_importers = new HashMap<Integer, Importer>();
		m_timing = timing; 
		
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
						Collection<String> dataSources = getAllowedDataSources();
						if(dataSources != null) { 
							importer = new NxImporter(dataSources);
						}	
						else 
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
//				else if (subfilePath.contains(".n3")) {
//					Importer importer = m_importers.get(Environment.NOTION3);
//					if (importer == null) {
//						importer = new N3Importer();
//						m_importers.put(Environment.NOTION3, importer);
//					}
//					importer.addImport(subfilePath);
//				} 
//				else if (subfilePath.endsWith(".rdf") || subfilePath.endsWith(".xml")) {
//					Importer importer = m_importers.get(Environment.RDFXML);
//					if (importer == null) {
//						importer = new RDFImporter();
//						m_importers.put(Environment.RDFXML, importer);
//					}
//					importer.addImport(subfilePath);
//				} 
//				else {
//					log.warn("unknown extension, assuming n-triples format");
//					Importer importer = m_importers.get(Environment.NTRIPLE);
//					if (importer == null) {
//						importer = new NxImporter();
//						m_importers.put(Environment.NTRIPLE, importer);
//					}
//					importer.addImport(subfilePath);
//				}
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
		log.info("-------------------- Creating Triple Table --------------------");
		long start = System.currentTimeMillis();
		initializeImporters();
		Statement stmt = m_dbService.createStatement();
		try {
			if (m_dbService.hasTable(Environment.TRIPLE_TABLE)) {
				stmt.execute("drop table " + Environment.TRIPLE_TABLE);
			}
			String createSql = "create table " + Environment.TRIPLE_TABLE + "( " + 
				Environment.TRIPLE_ID_COLUMN + " int unsigned not null primary key auto_increment, " + 
				Environment.TRIPLE_SUBJECT_COLUMN + " varchar(200) not null, " + 
				Environment.TRIPLE_SUBJECT_ID_COLUMN + " int unsigned not null default 0, " + 
				Environment.TRIPLE_PROPERTY_COLUMN + " varchar(200) not null, " + 
				Environment.TRIPLE_OBJECT_COLUMN + " varchar(200) not null, " + 
				Environment.TRIPLE_OBJECT_ID_COLUMN + " int unsigned not null default 0, " + 
				Environment.TRIPLE_PROPERTY_TYPE + " tinyint(1) unsigned not null, " + // 1:data property; 2:object property; 5:type; 6:rdf(s) property 
				Environment.TRIPLE_DS_ID_COLUMN + " smallint unsigned not null default 0, " +
				Environment.TRIPLE_DS_COLUMN + " varchar(100) not null) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + 
					" add index (" + Environment.TRIPLE_PROPERTY_TYPE + ")");
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + 
					" add index (" + Environment.TRIPLE_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + 
					" add index (" + Environment.TRIPLE_SUBJECT_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + 
					" add index (" + Environment.TRIPLE_OBJECT_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + 
					" add index (" + Environment.TRIPLE_DS_ID_COLUMN + ")");
			
			if(stmt != null)
				stmt.close();
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating triple table:");
			log.warn(ex.getMessage());
		} 
		
		log.info("-------------------- Importing Triples into Triple Table --------------------");
		DbTripleSink sink = new DbTripleSink();
		for(Importer importer : m_importers.values()) {
			importer.setTripleSink(sink);
			importer.doImport();
		}
		sink.close();
		
		long end = System.currentTimeMillis();
		log.info("Time for Creating Triple Table: " + (double)(end - start)/(double)1000 + "(sec)\n");
	}
	
	public void createDatasourceTable() {
		log.info("-------------------- Creating Datasource Table --------------------");
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
				Environment.DATASOURCE_FREQ_COLUMN + " int not null, " + 
				" index(" + Environment.DATASOURCE_ID_COLUMN + "), " + 
				" index(" + Environment.DATASOURCE_NAME_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
		
			log.info("-------------------- Populating Datasource Table --------------------");
			String insertSql = "insert into " + Environment.DATASOURCE_TABLE + "(" + 
				Environment.DATASOURCE_NAME_COLUMN + ", " + Environment.DATASOURCE_FREQ_COLUMN + ") ";
			
			String selectSql = "select " + Environment.TRIPLE_DS_COLUMN + ", count(*) as freq " + 
				" from " + Environment.TRIPLE_TABLE + 
				" group by " + Environment.TRIPLE_DS_COLUMN +
				" order by freq DESC"; 
			log.info("Step 1: inserting data sources into data source table");
			long t1 = System.currentTimeMillis();
			stmt.executeUpdate(insertSql + selectSql);
			long t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			String updateSql = "update " + Environment.TRIPLE_TABLE + " as A, " + 
					Environment.DATASOURCE_TABLE + " as B " +
				" set " + "A." + Environment.TRIPLE_DS_ID_COLUMN + " = " + "B." + 
					Environment.DATASOURCE_ID_COLUMN + 
				" where " + "A." + Environment.TRIPLE_DS_COLUMN + " = " + "B." + 
					Environment.DATASOURCE_NAME_COLUMN + 
				" and " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " <> " + Environment.UNPROCESSED;
			log.info("Step 2: updating data source id column of triple table"); 
			t1 = System.currentTimeMillis();
			stmt.executeUpdate(updateSql);
			t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Datasource Table: " + (double)(end - start)/(double)1000 + "(sec)\n");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating datasource table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createSchemaTable() {
		log.info("-------------------- Creating Schema Table --------------------");
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
				Environment.SCHEMA_FREQ_COLUMN + " int unsigned not null, " + 
				// 1: data property; 2: object property; 3: concept 4: top class
				Environment.SCHEMA_TYPE_COLUMN + " tinyint(1) unsigned not null, " + 
				Environment.SCHEMA_DS_ID_COLUMN + " smallint unsigned not null, " + 
				"index(" + Environment.SCHEMA_ID_COLUMN + "), " + 
				"index(" + Environment.SCHEMA_TYPE_COLUMN + "), " + 
				"index(" + Environment.SCHEMA_DS_ID_COLUMN + "), " + 
				"index(" + Environment.SCHEMA_URI_COLUMN + ")) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			
			log.info("-------------------- Populating Schema Table --------------------");
			String insertSql = "insert into " + Environment.SCHEMA_TABLE + "(" + 
				Environment.SCHEMA_URI_COLUMN + ", " +
				Environment.SCHEMA_FREQ_COLUMN + ", " +
				Environment.SCHEMA_TYPE_COLUMN + ", " + 
				Environment.SCHEMA_DS_ID_COLUMN + ") ";
			
			String selectSql = "select CONCAT('" + Environment.THING + 
						" (', " + Environment.DATASOURCE_NAME_COLUMN + ", ')'), " + 
					"0 " + ", " + // for top class, the frequency is not computed and simply set as 0.  
					Environment.TOP_CLASS + ", " + Environment.DATASOURCE_ID_COLUMN + 
				" from " + Environment.DATASOURCE_TABLE; 
			log.info("Step 1: inserting top class for each data source into schema table");
			long t1 = System.currentTimeMillis();
			stmt.executeUpdate(insertSql + selectSql);
			long t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			selectSql = "select " + Environment.TRIPLE_OBJECT_COLUMN + ", " + 
					"count(*) " + ", " + Environment.CONCEPT + ", " + Environment.TRIPLE_DS_ID_COLUMN + 
				" from " + Environment.TRIPLE_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.ENTITY_MEMBERSHIP_PROPERTY + 
				" group by " + Environment.TRIPLE_OBJECT_COLUMN + ", " + Environment.TRIPLE_DS_ID_COLUMN;
			log.info("Step 2: inserting concepts into schema table");
			t1 = System.currentTimeMillis();
			stmt.executeUpdate(insertSql + selectSql);
			t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			selectSql = "select " + Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
					"count(*) " + ", " + Environment.OBJECT_PROPERTY + ", " + Environment.TRIPLE_DS_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
				" group by " + Environment.TRIPLE_PROPERTY_COLUMN + ", " + Environment.TRIPLE_DS_ID_COLUMN;
			log.info("Step 3: inserting object properties into schema table");
			t1 = System.currentTimeMillis();
			stmt.executeUpdate(insertSql + selectSql);
			t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			selectSql = "select " + Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
					"count(*) " + ", " + Environment.DATA_PROPERTY + ", " +	Environment.TRIPLE_DS_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE +
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.DATA_PROPERTY + 
				" group by " + Environment.TRIPLE_PROPERTY_COLUMN + ", " + Environment.TRIPLE_DS_ID_COLUMN;
			log.info("Step 4: inserting data properties into schema table");
			t1 = System.currentTimeMillis();
			stmt.executeUpdate(insertSql + selectSql);
			t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Schema Table: " + (double)(end - start)/(double)1000 + "(sec)\n");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating schema table:");
			log.warn(ex.getMessage());
		}  
	}
	
	public void createEntityTable() {
		log.info("-------------------- Creating Entity Table --------------------");
        Statement stmt = m_dbService.createStatement();
        long start = System.currentTimeMillis();
        try {
			if (m_dbService.hasTable(Environment.ENTITY_TABLE)) {
				stmt.execute("drop table " + Environment.ENTITY_TABLE);
			}
			String createSql = "create table " + 
				Environment.ENTITY_TABLE + "( " + 
				Environment.ENTITY_ID_COLUMN + " int unsigned not null primary key auto_increment, " + 
				Environment.ENTITY_URI_COLUMN + " varchar(100) not null unique, " + 
				Environment.ENTITY_CONCEPT_ID_COLUMN + " mediumint unsigned not null default 0, " +
				Environment.ENTITY_DS_ID_COLUMN + " smallint unsigned not null) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.ENTITY_TABLE + 
					" add index (" + Environment.ENTITY_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.ENTITY_TABLE + 
					" add index (" + Environment.ENTITY_URI_COLUMN + ")");
			
			log.info("-------------------- Populating Entity Table --------------------");
			String insertSql = "insert IGNORE into " + Environment.ENTITY_TABLE + "(" + 
				Environment.ENTITY_URI_COLUMN + ", " + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
				Environment.ENTITY_DS_ID_COLUMN + ") "; 
			String selectSql = 	"select distinct " + Environment.TRIPLE_SUBJECT_COLUMN + ", " + 
					Environment.SCHEMA_ID_COLUMN + ", " + Environment.SCHEMA_DS_ID_COLUMN + 
				" from " + Environment.TRIPLE_TABLE + ", " + Environment.SCHEMA_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.ENTITY_MEMBERSHIP_PROPERTY + 
				" and " + Environment.TRIPLE_OBJECT_COLUMN + " = " + Environment.SCHEMA_URI_COLUMN +
				" and " + Environment.TRIPLE_DS_ID_COLUMN + " = " + Environment.SCHEMA_DS_ID_COLUMN; 
			log.info("Step 1: inserting entities with membership (not top class) into entity table");
			long t1 = System.currentTimeMillis();
			stmt.executeUpdate(insertSql + selectSql);
			long t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			selectSql = "select distinct " + Environment.TRIPLE_SUBJECT_COLUMN + ", " + 
					Environment.SCHEMA_ID_COLUMN + ", " + Environment.TRIPLE_DS_ID_COLUMN + 
				" from " + Environment.TRIPLE_TABLE + ", " + Environment.SCHEMA_TABLE +  
				" where (" + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
				" or " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.DATA_PROPERTY + ") " + 
				" and " + Environment.SCHEMA_TYPE_COLUMN + " = " + Environment.TOP_CLASS + 
				" and " + Environment.SCHEMA_DS_ID_COLUMN + " = " + Environment.TRIPLE_DS_ID_COLUMN;
			log.info("Step 2: inserting entities with membership (top class) into entity table");
			t1 = System.currentTimeMillis();
			stmt.executeUpdate(insertSql + selectSql);
			t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			String updateSql = "update " + Environment.TRIPLE_TABLE + " as A, " + 
					Environment.ENTITY_TABLE + " as B " +
				" set " + "A." + Environment.TRIPLE_SUBJECT_ID_COLUMN + " = " + 
					"B." + Environment.ENTITY_ID_COLUMN + 
				" where " + "A." + Environment.TRIPLE_SUBJECT_COLUMN + " = " + 
					"B." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " <> " + Environment.UNPROCESSED;
			log.info("Step 3: updating subject id column of triple table"); 
			t1 = System.currentTimeMillis();
			stmt.executeUpdate(updateSql);
			t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			updateSql = "update " + Environment.TRIPLE_TABLE + " as A, " + 	
					Environment.ENTITY_TABLE + " as B " +
				" set " + "A." + Environment.TRIPLE_OBJECT_ID_COLUMN + " = " + 
					"B." + Environment.ENTITY_ID_COLUMN + 
				" where " + "A." + Environment.TRIPLE_OBJECT_COLUMN + " = " + 
					"B." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY; 
			log.info("Step 4: updating object id column (entity) of triple table"); 
			t1 = System.currentTimeMillis();
			stmt.executeUpdate(updateSql);
			t2 = System.currentTimeMillis();
			log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Entity Table: " + (double)(end - start)/(double)1000 + "(sec)\n");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity table:");
			log.warn(ex.getMessage());
		} 
	} 
	
	public void createEntityRelationTable() {
		Statement stmt = m_dbService.createStatement();
        String R_1_ = Environment.ENTITY_RELATION_TABLE + 1 + "_"; 
        int maxDsId = getMaxDataSourceId();
        m_timing.init(maxDsId);
        int id = 1;
        try {
        	while(id <= maxDsId) {
        		long start = System.currentTimeMillis();
        		log.info("-------------------- Creating Entity Relation Table of Data Source " + id + " --------------------");
        		// Create Entity Relation Table 
        		String R_1 = R_1_ + id;
        		if (m_dbService.hasTable(R_1)) {
					stmt.execute("drop table " + R_1);
				}
				String createSql = "create table " + R_1 + "( " + 
					Environment.ENTITY_RELATION_UID_COLUMN + " int unsigned not null, " + 
					Environment.ENTITY_RELATION_VID_COLUMN + " int unsigned not null, " +
					Environment.ENTITY_RELATION_IGNORED_COLUMN + " boolean not null, " +
					"primary key("	+ Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
						Environment.ENTITY_RELATION_VID_COLUMN + ")) " + 
					"ENGINE=MyISAM";
				stmt.execute(createSql);
				
//				stmt.execute("alter table " + R_1 + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")");
//            	stmt.execute("alter table " + R_1 + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")");
//            	stmt.execute("alter table " + R_1 + " add index (" + Environment.ENTITY_RELATION_IGNORED_COLUMN + ")");
				
				stmt.execute("alter table " + R_1 + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")," + 
						" add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")," +
						" add index (" + Environment.ENTITY_RELATION_IGNORED_COLUMN + ")");
				
				log.info("-------------------- Populating Entity Relation Table of Data Source " + id + " --------------------");
				// Populate Entity Relation Table 
				String selectSql = "select distinct " + Environment.TRIPLE_SUBJECT_ID_COLUMN + ", " + 
						Environment.TRIPLE_OBJECT_ID_COLUMN + ", " + 
						Environment.SCHEMA_FREQ_COLUMN + " >= " + Environment.THRESHOLD_RELATION_FREQ_PRUNED +  
					" from " + Environment.TRIPLE_TABLE + ", " + Environment.SCHEMA_TABLE + 
					" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
					" and " + Environment.TRIPLE_DS_ID_COLUMN + " = " + id + 
					" and " + Environment.TRIPLE_OBJECT_ID_COLUMN + " <> 0 " + 
					" and " + Environment.TRIPLE_SUBJECT_ID_COLUMN + " <> 0 " + 
					" and " + Environment.TRIPLE_PROPERTY_COLUMN + " = " + Environment.SCHEMA_URI_COLUMN +
					" and " + Environment.SCHEMA_DS_ID_COLUMN  + " = " + id +
					" and " + Environment.SCHEMA_TYPE_COLUMN + " = " + Environment.OBJECT_PROPERTY;
				ResultSet rs = stmt.executeQuery(selectSql);
			
				String temp = m_config.getTemporaryDirectory() + m_config.getDbName() + "/entityRelation"; 
				BufferedWriter out = new BufferedWriter(new FileWriter(temp));
			
				int numTriples = 0;
				while (rs.next()) {
					if(++numTriples % 100000 == 0)
						log.debug("Processed Triples: " + numTriples);
            		int entityId1 = rs.getInt(1);
            		int entityId2 = rs.getInt(2);
            		int ignored = rs.getInt(3);
            		if(entityId1 < entityId2){
            			String str = entityId1 + "," + entityId2 + "," + ignored;
                    	out.write(str, 0, str.length());
                    	out.newLine();
            		}
            		else {
            			String str = entityId2 + "," + entityId1 + "," + ignored;
                    	out.write(str, 0, str.length());
                    	out.newLine();
            		}
            	}
            	if(rs != null)
            		rs.close();
            	out.close();
            
				String tempAlt = m_dbService.getMySQLFilepath(temp);

            	stmt.executeUpdate("load data local infile '" + tempAlt + "' " + 
            			" ignore into table " + R_1 + " fields terminated by ','");
            	File f = new File(temp);
            	if(!f.delete()) {
                	log.warn("Unable to delete tempdump");
                	System.exit(1);
            	}
            	
            	long end = System.currentTimeMillis();
     			log.info("Time for Creating Entity Relation Table of data source " + id + " " +
     					(double)(end - start)/(double)1000 + "(sec)\n");
     			m_timing.add(id, end-start);
     			
     			stmt.execute("flush tables");
     			id++;
        	}
			
			if(stmt != null)
            	stmt.close();
            
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity relation table:");
			log.warn(ex.getMessage());
		} catch (IOException ex) {
			log.warn("A warning in the process of creating entity relation table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createEntityRelationTable(int distance) {
        Statement stmt = m_dbService.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        String R_d_ = Environment.ENTITY_RELATION_TABLE + distance + "_"; 
        
        // To avoid entity relationship explosion for large distance
        boolean ignored = distance >= Environment.THRESHOLD_DISTANCE_PRUNED ? true : false;  
        
        int maxDsId = getMaxDataSourceId();
        m_timing.init(maxDsId);
		int id = 1;	
        try {
        	while(id <= maxDsId) {
        		long start = System.currentTimeMillis();
        		String R_d = R_d_ + id;
        		// Create Entity Relation Table at distance d
        		log.info("-------------------- Creating Entity Relation Table of Data Source " + id + " at distance " + distance + " --------------------");
        		if (m_dbService.hasTable(R_d)) {
        			stmt.execute("drop table " + R_d);
        		}
        		String createSql = "create table " + R_d + "( " + 
					Environment.ENTITY_RELATION_UID_COLUMN + " int unsigned not null, " + 
					Environment.ENTITY_RELATION_VID_COLUMN + " int unsigned not null, " +
					Environment.ENTITY_RELATION_IGNORED_COLUMN + " boolean not null, " +
					"primary key("	+ Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
						Environment.ENTITY_RELATION_VID_COLUMN + ")) " + 
						"ENGINE=MyISAM";
        		stmt.execute(createSql);
//				stmt.execute("alter table " + R_d + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")");
//        		stmt.execute("alter table " + R_d + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")");
//        		stmt.execute("alter table " + R_d + " add index (" + Environment.ENTITY_RELATION_IGNORED_COLUMN + ")");
        		
        		stmt.execute("alter table " + R_d + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + "), " +
        				" add index (" + Environment.ENTITY_RELATION_IGNORED_COLUMN + ")");
			
        		log.info("-------------------- Populating Entity Relation Table of Data Source " + id + " at distance " + distance + " --------------------");
        		// Populate Temporal Entity Relation Table at distance d
        		int num = 0;
        		long t1, t2;
			
        		if(distance == 2){
					log.info("Importing entity relationship for data source " + id + " at distance 2");
					String insertSql = "insert IGNORE into " + R_d + " "; 
					String R_1 = Environment.ENTITY_RELATION_TABLE + 1 + "_" + id;
					
					// R_1(u, v), R_1(u', v') -> R_2(u, v') where v = u'
					t1 = System.currentTimeMillis(); 
					String selectSql = "select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
							"(A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " or " + 
							"B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + ") " +
						" from " + R_1 + " as A, " + R_1 + " as B " +
						" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
							"B." + Environment.ENTITY_RELATION_UID_COLUMN;
					
					if(ignored) {
						selectSql += " and " + "A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1" + 
							" and " + "B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1";
					}
						
					num += stmt.executeUpdate(insertSql + selectSql);
					t2 = System.currentTimeMillis(); 
					log.info("Part 1: " + num + " entity relations of distance " + distance + " computed");
					log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
				
					// R_1(u, v), R_1(u', v') -> R_2(u, u') where v = v'
					t1 = System.currentTimeMillis(); 
					selectSql =	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
							"B." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
							"(A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " or " + 
							"B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + ") " +
						" from " + R_1 + " as A, " + R_1 + " as B " +
						" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN + 
							" and " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " < " + 
							"B." + Environment.ENTITY_RELATION_UID_COLUMN;
					
					if(ignored) {
						selectSql += " and " + "A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1" + 
							" and " + "B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1";
					}
					
					num += stmt.executeUpdate(insertSql + selectSql);
					t2 = System.currentTimeMillis(); 
					log.info("Part 2: " + num + " entity relations of distance " + distance + " computed");
					log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
				
					// R_1(u, v), R_1(u', v') -> R_2(v, v') where u = u'
					t1 = System.currentTimeMillis(); 
					selectSql =	"select " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
							"(A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " or " + 
							"B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + ") " +
						" from " + R_1 + " as A, " + R_1 + " as B " +
						" where " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
							"B." + Environment.ENTITY_RELATION_UID_COLUMN + 
							" and " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " < " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN;
					
					if(ignored || id == 3) {
						selectSql += " and " + "A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1" + 
							" and " + "B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1";
					}
					
					num += stmt.executeUpdate(insertSql + selectSql);
					t2 = System.currentTimeMillis(); 
					log.info("Part 3: " + num + " entity relations of distance " + distance + " computed");
					log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
				
					int deletedRows = 0;
					for(int i=1; i<distance; i++){
						String R_i = Environment.ENTITY_RELATION_TABLE + i + "_" + id;
						String deleteSql = "delete " + R_d + 
							" from " + R_d + ", " + R_i +
							" where " + R_d + "." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
								R_i + "." + Environment.ENTITY_RELATION_UID_COLUMN + 
								" and " + R_d + "." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
								R_i + "." + Environment.ENTITY_RELATION_VID_COLUMN; 
						deletedRows += stmt.executeUpdate(deleteSql);
					}
					log.info("Number of duplicated rows that are deleted: " + deletedRows + "\n");
        		}
			
        		if(distance >= 3){	
					log.info("Importing entity relationship for data source " + id + " at distance " + distance);
					String insertSql = "insert IGNORE into " + R_d + " "; 
					String R_1 = Environment.ENTITY_RELATION_TABLE + 1 + "_" + id;
					String R_d_minus_1 = Environment.ENTITY_RELATION_TABLE + (distance - 1) + "_" + id;
				
					// R_(d-1)(u, v), R_1(u', v') -> R_d(u, v') where v = u'
					t1 = System.currentTimeMillis(); 
					String selectSql = 	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
							"(A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " or " + 
							"B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + ") " +
						" from " + R_d_minus_1 + " as A, " + R_1 + " as B " +
						" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
							"B." + Environment.ENTITY_RELATION_UID_COLUMN;
					
					if(ignored) {
						selectSql += " and " + "A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1" + 
							" and " + "B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1";
					}
					
					num += stmt.executeUpdate(insertSql + selectSql);
					t2 = System.currentTimeMillis(); 
					log.info("Part 1: " + num + " entity relations of distance " + distance + " computed");
					log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
				
					// R_(d-1)(u, v), R_1(u', v') -> R_d(u', v) where u = v'
					t1 = System.currentTimeMillis(); 
					selectSql = "select " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
							"A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " +
							"(A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " or " + 
							"B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + ") " +
						" from " + R_d_minus_1 + " as A, " + R_1 + " as B " +
						" where " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN;
					
					if(ignored) {
						selectSql += " and " + "A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1" + 
							" and " + "B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1";
					}
					
					num += stmt.executeUpdate(insertSql + selectSql);
					t2 = System.currentTimeMillis(); 
					log.info("Part 2: " + num + " entity relations of distance " + distance + " computed");
					log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
				
					// R_(d-1)(u, v), R_1(u', v') -> R_d(u, u') where v = v'
					t1 = System.currentTimeMillis(); 
					selectSql =	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
							"B." + Environment.ENTITY_RELATION_UID_COLUMN + ", " +
							"(A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " or " + 
							"B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + ") " +
						" from " + R_d_minus_1 + " as A, " + R_1 + " as B " +
						" where " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN + 
							" and " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " < " + 
							"B." + Environment.ENTITY_RELATION_UID_COLUMN;
					
					if(ignored) {
						selectSql += " and " + "A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1" + 
							" and " + "B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1";
					}
					
					num += stmt.executeUpdate(insertSql + selectSql);
					t2 = System.currentTimeMillis(); 
					log.info("Part 3: " + num + " entity relations of distance " + distance + " computed");
					log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
				
					// R_(d-1)(u, v), R_1(u', v') -> R_d(v, v') where u = u'
					t1 = System.currentTimeMillis(); 
					selectSql =	"select " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
							"(A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " or " + 
							"B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + ") " +
						" from " + R_d_minus_1 + " as A, " + R_1 + " as B " +
						" where " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
							"B." + Environment.ENTITY_RELATION_UID_COLUMN + 
							" and " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + " < " + 
							"B." + Environment.ENTITY_RELATION_VID_COLUMN;
					
					if(ignored) {
						selectSql += " and " + "A." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1" + 
							" and " + "B." + Environment.ENTITY_RELATION_IGNORED_COLUMN + " <> 1";
					}
					
					num += stmt.executeUpdate(insertSql + selectSql);
					t2 = System.currentTimeMillis(); 
					log.info("Part 4: " + num + " entity relations of distance " + distance + " computed");
					log.info("time: " + (double)(t2 - t1)/(double)1000 + "(sec)");

					int deletedRows = 0;
					for(int i=1; i<distance; i++){
						String R_i = Environment.ENTITY_RELATION_TABLE + i + "_" + id;
						String deleteSql = "delete " + R_d + 
							" from " + R_d + ", " + R_i +
							" where " + R_d + "." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
								R_i + "." + Environment.ENTITY_RELATION_UID_COLUMN + " and " + 
								R_d + "." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
								R_i + "." + Environment.ENTITY_RELATION_VID_COLUMN; 
						deletedRows += stmt.executeUpdate(deleteSql);
					}
					log.info("Number of duplicated rows that are deleted: " + deletedRows + "\n");
				}
        		
        		long end = System.currentTimeMillis();
    			log.info("Time for Creating Entity Relation Table of Data Source " + id + " at distance " + distance + ": " + 
    					(double)(end - start)/(double)60000 + "(min)\n");
    			m_timing.add(id, end-start);
    			
    			stmt.execute("flush tables");
    			id++;
        	}
        	
			if(stmt != null)
				stmt.close();
			
			System.gc();
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity relation table at distance " + distance + ":");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createKeywordEntityLuceneIndex() {
		// construct keyword index using Lucene
		log.info("-------------------- Creating Keyword Index --------------------");
		long start = System.currentTimeMillis();
		
		String indexPath = m_config.getTemporaryDirectory() + "/" + m_config.getDbName() + "/lucene";
		File stopWords = new File("./res/en_stopWords");
		Analyzer analyzer = null;
		try {
//			analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT, stopWords);
//			analyzer = new StopAnalyzer(stopWords);
			analyzer = new PorterStemmerAnalyzer(Version.LUCENE_CURRENT, stopWords);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		// Store the index on disk:
		File dir = new File(indexPath);
		if(!dir.exists())
			dir.mkdirs();
		Directory directory;
		
        Statement stmt = m_dbService.createStatement();
		
		try {
			directory = FSDirectory.open(dir);
			IndexWriter iwriter = new IndexWriter(directory, analyzer, true, new IndexWriter.MaxFieldLength(20));
			
			// Statement for Entity Table
//			String selectEntitySql = "select " + Environment.ENTITY_ID_COLUMN + ", " + 
//					Environment.ENTITY_URI_COLUMN + 
//				" into outfile " + temp + " fields terminated by ',' lines terminated by '\n' " +
//				" from " + Environment.ENTITY_TABLE;
//			stmt.executeUpdate(selectEntitySql);
			
			// Statement for Triple Table
			String selectTripleSqlFw = "select " + Environment.TRIPLE_PROPERTY_TYPE + ", " + 
					Environment.TRIPLE_PROPERTY_COLUMN + ", " + Environment.TRIPLE_OBJECT_COLUMN + 
				" from " + Environment.TRIPLE_TABLE +
				" where " + Environment.TRIPLE_SUBJECT_ID_COLUMN + " = ?" + 
				" and (" + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.DATA_PROPERTY + 
				" or " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.RDFS_PROPERTY + ")";
			PreparedStatement psQueryTripleFw = m_dbService.createPreparedStatement(selectTripleSqlFw);
			ResultSet rsTriple = null;
			
			// Statement for Entity Table
			String selectEntitySql = "select " + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
					Environment.ENTITY_DS_ID_COLUMN + ", " + Environment.ENTITY_URI_COLUMN + 
				" from " + Environment.ENTITY_TABLE +
				" where " + Environment.ENTITY_ID_COLUMN + " = ?";
			PreparedStatement psQueryEntity = m_dbService.createPreparedStatement(selectEntitySql);
			ResultSet rsEntity = null;
			
			// processing each entity
//			String line;
//			BufferedReader br = new BufferedReader(new FileReader(temp));
//			while((line = br.readLine()) != null) {
//				String[] strs = line.split(",");
//				int entityId = Integer.valueOf(strs[0]);
//				String entityUri = strs[1];
			
			int entityId = 1;
			int maxEntityId = getMaxEntityId();
			while(entityId <= maxEntityId) {
				if(entityId % 10000 == 0)
					log.debug("Processed Entities: " + entityId);
				
				psQueryEntity.setInt(1, entityId);
				rsEntity = psQueryEntity.executeQuery();
				int conceptId = 0;
				int dsId = 0;
				String uri = ""; 
				if(rsEntity.next()) {
					conceptId = rsEntity.getInt(Environment.ENTITY_CONCEPT_ID_COLUMN);
					dsId = rsEntity.getInt(Environment.ENTITY_DS_ID_COLUMN);
					uri = rsEntity.getString(Environment.ENTITY_URI_COLUMN); 
				}
				if(rsEntity != null)
					rsEntity.close();
				
//				String terms = trucateUri(entityUri) + " ";
				StringBuffer terms = new StringBuffer(); 
				// processing forward edges
				psQueryTripleFw.setInt(1, entityId);
				rsTriple = psQueryTripleFw.executeQuery();
				while (rsTriple.next()) {
					String term = rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN) + " ";
					if(term.startsWith("http"))
						continue;
					int type = rsTriple.getInt(Environment.TRIPLE_PROPERTY_TYPE);
					if(type == Environment.DATA_PROPERTY) {
						// term for data property 
						terms.append(term);
					}
					else if(type == Environment.RDFS_PROPERTY) {
						// term for label property 
						String property = rsTriple.getString(Environment.TRIPLE_PROPERTY_COLUMN); 
						if(property.equals(RDFS.LABEL.stringValue())) {
							terms.append(term);
						}
					}
				}
				if(rsTriple != null)
					rsTriple.close();
				
				Document doc = new Document();
				doc.add(new Field(Environment.FIELD_ENTITY_ID, Integer.toString(entityId), 
						Field.Store.YES, Field.Index.NO));
				doc.add(new Field(Environment.FIELD_CONCEPT_ID, Integer.toString(conceptId), 
						Field.Store.YES, Field.Index.NOT_ANALYZED));
				doc.add(new Field(Environment.FIELD_DS_ID, Integer.toString(dsId), 
						Field.Store.YES, Field.Index.NOT_ANALYZED));
				doc.add(new Field(Environment.FIELD_ENTITY_URI, uri, 
						Field.Store.YES, Field.Index.NOT_ANALYZED));
				if(!terms.equals(""))
					doc.add(new Field(Environment.FIELD_TERM_LITERAL, terms.toString(), 
							Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
				iwriter.addDocument(doc);
				
				entityId++;
			}	
			
			if(psQueryTripleFw != null)
				psQueryTripleFw.close();
			if(stmt != null)
				stmt.close();
			
			if(psQueryEntity != null)
				psQueryEntity.close();
			
			iwriter.close();
			directory.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Keyword Index: " + (double)(end - start)/(double)1000 + "(sec)\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createCompleteKeywordTable() {
		log.info("-------------------- Creating Keyword Entity Inclusion Table and Complete Keyword Table --------------------");
		long start = System.currentTimeMillis();
		
		Statement stmt = m_dbService.createStatement();
		try {
			if (m_dbService.hasTable(Environment.COMPLETE_KEYWORD_TABLE)) {
				stmt.execute("drop table " + Environment.COMPLETE_KEYWORD_TABLE);
			}
			String createSql = "create table " + Environment.COMPLETE_KEYWORD_TABLE + "( " + 
				// It might be entity id or keyword id, therefore not unique.    
				Environment.COMPLETE_ID_COLUMN + " int unsigned not null, " + 
				Environment.COMPLETE_KEYWORD_COLUMN + " varchar(100) not null primary key, " + 
				// compound -- appears only in one entity; single -- appears in more than one entity
				Environment.COMPLETE_TYPE_COLUMN + " tinyint unsigned not null, " +  
				Environment.COMPLETE_FREQ_COLUMN + " smallint unsigned not null) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.COMPLETE_KEYWORD_TABLE + 
					" add index (" + Environment.COMPLETE_KEYWORD_COLUMN + ")");
			stmt.execute("alter table " + Environment.COMPLETE_KEYWORD_TABLE + 
					" add index (" + Environment.COMPLETE_TYPE_COLUMN + ")");
			
			if (m_dbService.hasTable(Environment.KEYWORD_ENTITY_INCLUSION_TABLE)) {
				stmt.execute("drop table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE);
			}
			createSql = "create table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + "( " + 
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " float unsigned not null, " + 
				"primary key(" + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + 
					" add index (" + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + 
					" add index (" + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ")");
			
			log.info("-------------------- Populating Keyword Entity Inclusion Table and Keyword Table --------------------");
			// Statement for Keyword Table
			String insertKeywSql = "insert into " + Environment.COMPLETE_KEYWORD_TABLE + " values(?, ?, ?, ?)"; 
			PreparedStatement psInsertKeyw = m_dbService.createPreparedStatement(insertKeywSql);
			
			// Statement for Keyword Entity Inclusion Table
			String insertKeywEntitySql = "insert IGNORE into " + 
				Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " values(?, ?, ?)"; 
			PreparedStatement psInsertKeywEntity = m_dbService.createPreparedStatement(insertKeywEntitySql);
			
			// Retrieve Keywords from Lucene Index
			Directory directory = FSDirectory.open(new File(m_config.getTemporaryDirectory() + "/" + 
				m_config.getDbName() + "/lucene"));
			IndexReader ireader = IndexReader.open(directory, true);
			
			int numDocs = ireader.numDocs();
			
			// For Test
			PrintWriter pw_k = new PrintWriter(m_config.getTemporaryDirectory() + "/" + 
				m_config.getDbName() +  "/keyword." + m_config.getDbName()); 
			Map<String,Integer> keyword_freq = new HashMap<String,Integer>();
			PrintWriter pw_kf = new PrintWriter(m_config.getTemporaryDirectory() + "/" + 
				m_config.getDbName() +  "/keyword_freq." + m_config.getDbName()); 
			
			int numKeyw = 0;
			int keywordId = 0;
			TermEnum tEnum = ireader.terms();
			while(tEnum.next()) {
				
				Term term = tEnum.term();
				int docFreq = tEnum.docFreq();
				String text = term.text();
				
				// to be removed later
				if(!containsOnlyLetters(text))
					continue;
				
				numKeyw++;
				// For Test
				pw_k.print(numKeyw + ": " + text + "\t docFreq: " + docFreq);
				pw_k.println();
				keyword_freq.put(text,docFreq);
				
				if(numKeyw % 10000 == 0)
					log.debug("Processed Keywords: " + numKeyw);
				
				if(docFreq == 1) {
					TermDocs tDocs = ireader.termDocs(term);
					if(tDocs.next()) {
						int docID = tDocs.doc();
						Document doc = ireader.document(docID);
						int entityId = Integer.valueOf(doc.get(Environment.FIELD_ENTITY_ID)); 
						psInsertKeyw.setInt(1, entityId);
						psInsertKeyw.setString(2, text);
						psInsertKeyw.setInt(3, Environment.TERM_COMPOUND); // compound keyword appears only in one entity
						psInsertKeyw.setInt(4, docFreq);
						psInsertKeyw.executeUpdate();
					}
				}
				else if(docFreq < 65535) {
					keywordId++;
					psInsertKeyw.setInt(1, keywordId);
					psInsertKeyw.setString(2, text);
					psInsertKeyw.setInt(3, Environment.TERM_SINGLE); // single keyword appears in more than one entity
					psInsertKeyw.setInt(4, docFreq);
					psInsertKeyw.executeUpdate();
					
					TermDocs tDocs = ireader.termDocs(term);
					while(tDocs.next()) {
						int docID = tDocs.doc();
						int termFreqInDoc = tDocs.freq();
						int docFreqOfTerm = ireader.docFreq(term);
						float score = (float)((1 + Math.log(1 + Math.log(termFreqInDoc)))*Math.log((numDocs + 1)/docFreqOfTerm));
						
						Document doc = ireader.document(docID);
						int entityId = Integer.valueOf(doc.get(Environment.FIELD_ENTITY_ID)); 
						
						psInsertKeywEntity.setInt(1, keywordId);
						psInsertKeywEntity.setInt(2, entityId);
						psInsertKeywEntity.setFloat(3, score);
						psInsertKeywEntity.executeUpdate();
					}
				}
			}
			
			ireader.close();
			directory.close();

			// For Test
			Map<String,Integer> keyword_freq_sorted = sortByValue(keyword_freq); 
			for(String keyword : keyword_freq_sorted.keySet()) {
				pw_kf.println(keyword + "\t docFreq: " + keyword_freq_sorted.get(keyword));
			}
			pw_k.close();
			pw_kf.close();

			long end = System.currentTimeMillis();
			log.info("Time for Creating Keyword Entity Inclusion Table and Complete Keyword Table: " + 
					(double) (end - start) / (double)1000  + "(sec)\n");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating keyword entity inclusion table and complete keyword table:");
			log.warn(ex.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}

	public void createKeywordConceptConnectionTable() {
		log.info("-------------------- Creating Keyword Concept Connection Table --------------------");
		long start = System.currentTimeMillis();
		
        Statement stmt = m_dbService.createStatement();
        String R = Environment.ENTITY_RELATION_TABLE; 
        try {
        	// Create Keyword Concept Connection Table 
			if (m_dbService.hasTable(Environment.KEYWORD_CONCEPT_CONNECTION_TABLE)) {
				stmt.execute("drop table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE);
			}
			String createSql = "create table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + "( " + 
				Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + " int unsigned not null, " + 
				Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + " int unsigned not null, " +
				Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + " mediumint unsigned not null, " + 
				Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + " mediumint unsigned not null, " +
				Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + " smallint unsigned not null, " + 
				Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + " smallint unsigned not null, " +
				Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " float unsigned not null, " + 
				Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN + " tinyint(1) unsigned not null, " + // 0: sc; 1: ss; 2: cc
				"primary key("	+ Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN +  ", " +
					Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			
			log.info("-------------------- Populating Keyword Concept Connection Table --------------------");
			// Populate Keyword Concept Connection Table 
			int maxEntityId = getMaxEntityId();
			
			String insertSql = "insert into " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + " ";
			String onUpdateSql = " on duplicate key update " + 
				Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " = " + 
				Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " + " + 
				"VALUES(" + Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + ")"; 
			String selectSql;
			float compoundKeywordScore = (float)(2*Math.log(maxEntityId + 1));
			
			int numConnection = 0;
			// Keyword Concept Connection at distance 0 
			// single keyword and compound keyword at distance 0
			selectSql = " select " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				"KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ", " +
				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
				"sum(" + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " + " + compoundKeywordScore + ") " + ", " + 
				Environment.TERM_PAIR_SINGLE_COMPOUND + // single keyword and compound keyword -- keyword id and entity id
				" from " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE, " + 
					Environment.ENTITY_TABLE + " as E " +
				" where " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
					"E." + Environment.ENTITY_ID_COLUMN + 
				" group by " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 	
					"KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN;  
			long t1 = System.currentTimeMillis();
			numConnection += stmt.executeUpdate(insertSql + selectSql);
			long t2 = System.currentTimeMillis();
			log.info(numConnection + "\t single and compound keywords\t distance: " + 0 + "\t " + 
					"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			// single keyword and single keyword at distance 0
			selectSql = " select " + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				"KE2." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
				"sum(" + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " + " + 
					"KE2." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ") " +  ", " +
				Environment.TERM_PAIR_SINGLE_SINGLE + // single keyword and single keyword -- keyword id and keyword id
				" from " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE1, " + 
					Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE2, " +			
					Environment.ENTITY_TABLE + " as E " +
				" where " + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
					"E." + Environment.ENTITY_ID_COLUMN + 
				" and " + "KE2." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
					"E." + Environment.ENTITY_ID_COLUMN + 
				" and " + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + " < " + 
					"KE2." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + 
				" group by " + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
					"KE2." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
					"E." + Environment.ENTITY_CONCEPT_ID_COLUMN;  
			t1 = System.currentTimeMillis();
			numConnection += stmt.executeUpdate(insertSql + selectSql);
			t2 = System.currentTimeMillis();
			log.info(numConnection + "\t single and single keywords\t distance: " + 0 + "\t " + 
					"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			// compound keyword and compound keyword at distance 0 
			selectSql = " select " + "E." + Environment.ENTITY_ID_COLUMN + ", " + 
				"E." + Environment.ENTITY_ID_COLUMN + ", " +
				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
				"sum( 2*" + compoundKeywordScore + "), " + 
				Environment.TERM_PAIR_COMPOUND_COMPOUND + // compound keyword and compound keyword -- entity id and entity id 
				" from " + Environment.ENTITY_TABLE + " as E " + 
				" group by " + "E." + Environment.ENTITY_ID_COLUMN;  
			t1 = System.currentTimeMillis();
			numConnection += stmt.executeUpdate(insertSql + selectSql);
			t2 = System.currentTimeMillis();
			log.info(numConnection + "\t compound and compound keywords\t distance: " + 0 + "\t " + 
					"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			
			
			// Keyword Concept Connection at distance from 1 to maximum distance
			int maxDistance = m_config.getMaxDistance();
			int maxDsId = getMaxDataSourceId();
			for(int i = 1; i <= maxDistance; i++) {
				String R_i_ = R + i + "_";
				
				// single keyword and compound keyword at distance i
				t1 = System.currentTimeMillis();
				String temp_sc = "temp_table_sc";
				if (m_dbService.hasTable(temp_sc)) {
					stmt.execute("drop table " + temp_sc);
				}
				createSql = "create table " + temp_sc + "( " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + " int unsigned not null, " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + " int unsigned not null, " +
					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + " mediumint unsigned not null, " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + " mediumint unsigned not null, " +
					Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + " smallint unsigned not null, " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + " smallint unsigned not null, " +
					Environment.KEYWORD_CONCEPT_CONNECTION_FREQ_COLUMN + " smallint unsigned not null, " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " float unsigned not null) " + 
					"ENGINE=MyISAM";
				stmt.execute(createSql);
				
				String insertSqlTemp = "insert into " + temp_sc + " ";
				int num = 0;
				int id = 1;
				while (id <= 194){
					String R_i = R_i_ + id; 
					
					selectSql = " select " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
							"ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
							"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
							"E2." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
							"E1." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
							"E2." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
							"count(*), " + 
							"sum(" + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ") " +  
						" from " + R_i + " as ER, " + 
							Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE, " + 
							Environment.ENTITY_TABLE + " as E1, " + 
							Environment.ENTITY_TABLE + " as E2 " +
						" where " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
							"ER." + Environment.ENTITY_RELATION_VID_COLUMN + 
						" and " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
							"E1." + Environment.ENTITY_ID_COLUMN + 
						" and " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
							"E2." + Environment.ENTITY_ID_COLUMN + 
						" and " + "E1." + Environment.ENTITY_DS_ID_COLUMN + " = " + id + 
//						" and " + "E2." + Environment.ENTITY_DS_ID_COLUMN + " = " + id + 
						" group by " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 	
							"ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
							"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN;  
					num += stmt.executeUpdate(insertSqlTemp + selectSql);
					log.debug("Part 1 of data source " + id + ": " + num);
					
					id++;
				}
				id = 1;
				num = 0; 
				while (id <= 194){
					String R_i = R_i_ + id; 
					
					selectSql = " select " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
							"ER." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
							"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
							"E2." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
							"E1." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
							"E2." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
							"count(*), " + 
							"sum(" + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ") " +    
						" from " + R_i + " as ER, " + 
							Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE, " + 
							Environment.ENTITY_TABLE + " as E1, " + 
							Environment.ENTITY_TABLE + " as E2 " +
						" where " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
							"ER." + Environment.ENTITY_RELATION_UID_COLUMN + 
						" and " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
							"E1." + Environment.ENTITY_ID_COLUMN + 
						" and " + "ER." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
							"E2." + Environment.ENTITY_ID_COLUMN + 
						" and " + "E1." + Environment.ENTITY_DS_ID_COLUMN + " = " + id + 
//						" and " + "E2." + Environment.ENTITY_DS_ID_COLUMN + " = " + id +
						" group by " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 	
							"ER." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
							"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN;  
					num += stmt.executeUpdate(insertSqlTemp + selectSql);
					log.debug("Part 2 of data source " + id + ": " + num);
				
					id++;
				}
				t2 = System.currentTimeMillis();
				log.info("temp" + "\t single and compound keywords\t distance: " + i + "\t " + 
						"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
				selectSql = " select " + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " +
					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + ", " +
					Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + ", " + 
					Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + ", " +
					"sum((" + Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " + " + 
						Environment.KEYWORD_CONCEPT_CONNECTION_FREQ_COLUMN + " * " + compoundKeywordScore +
						")/(" + i + "+1)), " +  
					Environment.TERM_PAIR_SINGLE_COMPOUND + // single keyword and compound keyword -- keyword id and entity id
					" from " + temp_sc + 
					" group by " + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
						Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " +
						Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN;  
				t1 = System.currentTimeMillis();
				numConnection += stmt.executeUpdate(insertSql + selectSql + onUpdateSql);
				t2 = System.currentTimeMillis();
				log.info(numConnection + "\t single and compound keywords\t distance: " + i + "\t " + 
						"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
					
				// single keyword and single keyword at distance i
				String temp_ss = "temp_table_ss";
				if (m_dbService.hasTable(temp_ss)) {
					stmt.execute("drop table " + temp_ss);
				}
				t1 = System.currentTimeMillis();
				id = 1;
				while (id <= 194){
					selectSql = " select " + "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
						"KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " +
						"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
						"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + ", " +
						"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + ", " + 
						"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + ", " +
						"(sum(KE." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ") + " + 
						 	"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + 
							")/(" + i + "+1), " +
						Environment.TERM_PAIR_SINGLE_SINGLE + // single keyword and single keyword -- keyword id and keyword id
						" from " + temp_sc + " as SC, " + 
							Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE " +
						" where " + "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + " = " + 
							"KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + 
						" and " + "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + " < " + 
							"KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + 
						" and " +  "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + " = " + id + 
//						" and " +  "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + " = " + id + 
						" group by " + "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
							"KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
							"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
							"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN;  
					numConnection += stmt.executeUpdate(insertSql + selectSql + onUpdateSql);
					
					id++;
				}
				
				if (m_dbService.hasTable(temp_sc)) {
					stmt.execute("drop table " + temp_sc);
				}
				t2 = System.currentTimeMillis();
				log.info(numConnection + "\t single and single keywords\t distance: " + i + "\t " + 
						"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
				
				// compound keyword and compound keyword at distance i  
				t1 = System.currentTimeMillis();
				id = 1;
				while (id <= 194){
					String R_i = R_i_ + id;
					selectSql = " select " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " +
						"ER." + Environment.ENTITY_RELATION_VID_COLUMN + ", " +
						"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
						"E2." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
						"E1." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
						"E2." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
						"(" + "2*" + compoundKeywordScore + ")/(" + i + "+1), " + 
						Environment.TERM_PAIR_COMPOUND_COMPOUND + // compound keyword and compound keyword -- entity id and entity id 
						" from " + R_i + " as ER, " +	 	
							Environment.ENTITY_TABLE + " as E1, " + 
							Environment.ENTITY_TABLE + " as E2 " +
						" where " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
							"E1." + Environment.ENTITY_ID_COLUMN + 
						" and " + "ER." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
							"E2." + Environment.ENTITY_ID_COLUMN; 
//						" group by " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
//							"ER." + Environment.ENTITY_RELATION_VID_COLUMN;
					numConnection += stmt.executeUpdate(insertSql + selectSql + onUpdateSql);
					
					id++;
				}
				
				t2 = System.currentTimeMillis();
				log.info(numConnection + "\tcompound and compound keywords\tdistance: " + i + "\t " + 
						"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
			}
			
			stmt.execute("alter table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + 
					" DROP PRIMARY KEY");
			stmt.execute("alter table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + 
					" add index (" + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " +
									Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " +
									Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN + ")");
			
			if(stmt != null)
            	stmt.close();
            
            long end = System.currentTimeMillis();
			log.info("Time for Creating Keyword Concept Connection Table: " + 
					(double)(end - start)/(double)1000 + "(sec)\n");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating keyword concept connection table:");
			log.warn(ex.getMessage());
		} 
	} 
	
//	public void createKeywordConceptConnectionTable() {
//		log.info("-------------------- Creating Keyword Concept Connection Table --------------------");
//		long start = System.currentTimeMillis();
//		
//        Statement stmt = m_dbService.createStatement();
//        String R = Environment.ENTITY_RELATION_TABLE; 
//        try {
//        	// Create Keyword Concept Connection Table 
//			if (m_dbService.hasTable(Environment.KEYWORD_CONCEPT_CONNECTION_TABLE)) {
//				stmt.execute("drop table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE);
//			}
//			String createSql = "create table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + "( " + 
//				Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + " int unsigned not null, " + 
//				Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + " int unsigned not null, " +
//				Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + " mediumint unsigned not null, " + 
//				Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + " mediumint unsigned not null, " +
//				Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + " smallint unsigned not null, " + 
//				Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + " smallint unsigned not null, " +
//				Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " float unsigned not null, " + 
//				Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN + " tinyint(1) unsigned not null, " + // 0: sc; 1: ss; 2: cc
//				"primary key("	+ Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN +  ", " +
//					Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN + ")) " + 
//				"ENGINE=MyISAM";
//			stmt.execute(createSql);
//			
//			log.info("-------------------- Populating Keyword Concept Connection Table --------------------");
//			// Populate Keyword Concept Connection Table 
//			int maxEntityId = getMaxEntityId();
//			
//			String insertSql = "insert into " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + " ";
//			String onUpdateSql = " on duplicate key update " + 
//				Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " = " + 
//				Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " + " + 
//				"VALUES(" + Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + ")"; 
//			String selectSql;
//			float compoundKeywordScore = (float)(2*Math.log(maxEntityId + 1));
//			
//			int numConnection = 0;
//			// Keyword Concept Connection at distance 0 
//			// single keyword and compound keyword at distance 0
//			selectSql = " select " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//				"KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ", " +
//				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
//				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
//				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//				"sum(" + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " + " + compoundKeywordScore + ") " + ", " + 
//				Environment.TERM_PAIR_SINGLE_COMPOUND + // single keyword and compound keyword -- keyword id and entity id
//				" from " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE, " + 
//					Environment.ENTITY_TABLE + " as E " +
//				" where " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
//					"E." + Environment.ENTITY_ID_COLUMN + 
//				" group by " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 	
//					"KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN;  
//			long t1 = System.currentTimeMillis();
//			numConnection += stmt.executeUpdate(insertSql + selectSql);
//			long t2 = System.currentTimeMillis();
//			log.info(numConnection + "\t single and compound keywords\t distance: " + 0 + "\t " + 
//					"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
//			
//			// single keyword and single keyword at distance 0
//			selectSql = " select " + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//				"KE2." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
//				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
//				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//				"sum(" + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " + " + 
//					"KE2." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ") " +  ", " +
//				Environment.TERM_PAIR_SINGLE_SINGLE + // single keyword and single keyword -- keyword id and keyword id
//				" from " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE1, " + 
//					Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE2, " +			
//					Environment.ENTITY_TABLE + " as E " +
//				" where " + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
//					"E." + Environment.ENTITY_ID_COLUMN + 
//				" and " + "KE2." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
//					"E." + Environment.ENTITY_ID_COLUMN + 
//				" and " + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + " < " + 
//					"KE2." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + 
//				" group by " + "KE1." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//					"KE2." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//					"E." + Environment.ENTITY_CONCEPT_ID_COLUMN;  
//			t1 = System.currentTimeMillis();
//			numConnection += stmt.executeUpdate(insertSql + selectSql);
//			t2 = System.currentTimeMillis();
//			log.info(numConnection + "\t single and single keywords\t distance: " + 0 + "\t " + 
//					"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
//			
//			// compound keyword and compound keyword at distance 0 
//			selectSql = " select " + "E." + Environment.ENTITY_ID_COLUMN + ", " + 
//				"E." + Environment.ENTITY_ID_COLUMN + ", " +
//				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
//				"E." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
//				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//				"E." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//				"sum( 2*" + compoundKeywordScore + "), " + 
//				Environment.TERM_PAIR_COMPOUND_COMPOUND + // compound keyword and compound keyword -- entity id and entity id 
//				" from " + Environment.ENTITY_TABLE + " as E " + 
//				" group by " + "E." + Environment.ENTITY_ID_COLUMN;  
//			t1 = System.currentTimeMillis();
//			numConnection += stmt.executeUpdate(insertSql + selectSql);
//			t2 = System.currentTimeMillis();
//			log.info(numConnection + "\t compound and compound keywords\t distance: " + 0 + "\t " + 
//					"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
//			
//			
//			// Keyword Concept Connection at distance from 1 to maximum distance
//			int maxDistance = m_config.getMaxDistance();
//			int maxDsId = getMaxDataSourceId();
//			for(int i = 1; i <= maxDistance; i++) {
//				String R_i_ = R + i + "_";
//				
//				// single keyword and compound keyword at distance i
//				t1 = System.currentTimeMillis();
//				String temp_sc = "temp_table_sc";
//				if (m_dbService.hasTable(temp_sc)) {
//					stmt.execute("drop table " + temp_sc);
//				}
//				createSql = "create table " + temp_sc + "( " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + " int unsigned not null, " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + " int unsigned not null, " +
//					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + " mediumint unsigned not null, " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + " mediumint unsigned not null, " +
//					Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + " smallint unsigned not null, " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + " smallint unsigned not null, " +
//					Environment.KEYWORD_CONCEPT_CONNECTION_FREQ_COLUMN + " smallint unsigned not null, " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " float unsigned not null) " + 
//					"ENGINE=MyISAM";
//				stmt.execute(createSql);
//				
//				String insertSqlTemp = "insert into " + temp_sc + " ";
//				int num = 0;
//				int id = 1;
//				while (id <= 194){
//					String R_i = R_i_ + id; 
//					
//					selectSql = " select " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//							"ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
//							"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
//							"E2." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
//							"E1." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//							"E2." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//							"count(*), " + 
//							"sum(" + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ") " +  
//						" from " + R_i + " as ER, " + 
//							Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE, " + 
//							Environment.ENTITY_TABLE + " as E1, " + 
//							Environment.ENTITY_TABLE + " as E2 " +
//						" where " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
//							"ER." + Environment.ENTITY_RELATION_VID_COLUMN + 
//						" and " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
//							"E1." + Environment.ENTITY_ID_COLUMN + 
//						" and " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
//							"E2." + Environment.ENTITY_ID_COLUMN + 
//						" and " + "E1." + Environment.ENTITY_DS_ID_COLUMN + " = " + id + 
////						" and " + "E2." + Environment.ENTITY_DS_ID_COLUMN + " = " + id + 
//						" group by " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 	
//							"ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
//							"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN;  
//					num += stmt.executeUpdate(insertSqlTemp + selectSql);
//					log.debug("Part 1 of data source " + id + ": " + num);
//					
//					id++;
//				}
//				id = 1;
//				num = 0; 
//				while (id <= 194){
//					String R_i = R_i_ + id; 
//					
//					selectSql = " select " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//							"ER." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
//							"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
//							"E2." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
//							"E1." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//							"E2." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//							"count(*), " + 
//							"sum(" + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ") " +    
//						" from " + R_i + " as ER, " + 
//							Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE, " + 
//							Environment.ENTITY_TABLE + " as E1, " + 
//							Environment.ENTITY_TABLE + " as E2 " +
//						" where " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
//							"ER." + Environment.ENTITY_RELATION_UID_COLUMN + 
//						" and " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " = " + 
//							"E1." + Environment.ENTITY_ID_COLUMN + 
//						" and " + "ER." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
//							"E2." + Environment.ENTITY_ID_COLUMN + 
//						" and " + "E1." + Environment.ENTITY_DS_ID_COLUMN + " = " + id + 
////						" and " + "E2." + Environment.ENTITY_DS_ID_COLUMN + " = " + id +
//						" group by " + "KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 	
//							"ER." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + 
//							"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN;  
//					num += stmt.executeUpdate(insertSqlTemp + selectSql);
//					log.debug("Part 2 of data source " + id + ": " + num);
//				
//					id++;
//				}
//				t2 = System.currentTimeMillis();
//				log.info("temp" + "\t single and compound keywords\t distance: " + i + "\t " + 
//						"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
//				selectSql = " select " + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " +
//					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + ", " +
//					Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + ", " + 
//					Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + ", " +
//					"sum((" + Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + " + " + 
//						Environment.KEYWORD_CONCEPT_CONNECTION_FREQ_COLUMN + " * " + compoundKeywordScore +
//						")/(" + i + "+1)), " +  
//					Environment.TERM_PAIR_SINGLE_COMPOUND + // single keyword and compound keyword -- keyword id and entity id
//					" from " + temp_sc + 
//					" group by " + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
//						Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " +
//						Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN;  
//				t1 = System.currentTimeMillis();
//				numConnection += stmt.executeUpdate(insertSql + selectSql + onUpdateSql);
//				t2 = System.currentTimeMillis();
//				log.info(numConnection + "\t single and compound keywords\t distance: " + i + "\t " + 
//						"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
//					
//				// single keyword and single keyword at distance i
//				String temp_ss = "temp_table_ss";
//				if (m_dbService.hasTable(temp_ss)) {
//					stmt.execute("drop table " + temp_ss);
//				}
//				t1 = System.currentTimeMillis();
//				id = 1;
//				while (id <= 194){
//					selectSql = " select " + "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
//						"KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " +
//						"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
//						"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + ", " +
//						"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + ", " + 
//						"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + ", " +
//						"(sum(KE." + Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + ") + " + 
//						 	"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_SCORE_COLUMN + 
//							")/(" + i + "+1), " +
//						Environment.TERM_PAIR_SINGLE_SINGLE + // single keyword and single keyword -- keyword id and keyword id
//						" from " + temp_sc + " as SC, " + 
//							Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " as KE " +
//						" where " + "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + " = " + 
//							"KE." + Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + 
//						" and " + "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + " < " + 
//							"KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + 
//						" and " +  "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_DS_UID_COLUMN + " = " + id + 
////						" and " +  "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_DS_VID_COLUMN + " = " + id + 
//						" group by " + "SC." + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " + 
//							"KE." + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//							"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
//							"SC." + Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN;  
//					numConnection += stmt.executeUpdate(insertSql + selectSql + onUpdateSql);
//					
//					id++;
//				}
//				
//				if (m_dbService.hasTable(temp_sc)) {
//					stmt.execute("drop table " + temp_sc);
//				}
//				t2 = System.currentTimeMillis();
//				log.info(numConnection + "\t single and single keywords\t distance: " + i + "\t " + 
//						"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
//				
//				// compound keyword and compound keyword at distance i  
//				t1 = System.currentTimeMillis();
//				id = 1;
//				while (id <= 194){
//					String R_i = R_i_ + id;
//					selectSql = " select " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " +
//						"ER." + Environment.ENTITY_RELATION_VID_COLUMN + ", " +
//						"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
//						"E2." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
//						"E1." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//						"E2." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//						"(" + "2*" + compoundKeywordScore + ")/(" + i + "+1), " + 
//						Environment.TERM_PAIR_COMPOUND_COMPOUND + // compound keyword and compound keyword -- entity id and entity id 
//						" from " + R_i + " as ER, " +	 	
//							Environment.ENTITY_TABLE + " as E1, " + 
//							Environment.ENTITY_TABLE + " as E2 " +
//						" where " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
//							"E1." + Environment.ENTITY_ID_COLUMN + 
//						" and " + "ER." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
//							"E2." + Environment.ENTITY_ID_COLUMN; 
////						" group by " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
////							"ER." + Environment.ENTITY_RELATION_VID_COLUMN;
//					numConnection += stmt.executeUpdate(insertSql + selectSql + onUpdateSql);
//					
//					id++;
//				}
//				
//				t2 = System.currentTimeMillis();
//				log.info(numConnection + "\tcompound and compound keywords\tdistance: " + i + "\t " + 
//						"time: " + (double)(t2 - t1)/(double)1000 + "(sec)");
//			}
//			
//			stmt.execute("alter table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + 
//					" DROP PRIMARY KEY");
//			stmt.execute("alter table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + 
//					" add index (" + Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " +
//									Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " +
//									Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN + ")");
//			
//			if(stmt != null)
//            	stmt.close();
//            
//            long end = System.currentTimeMillis();
//			log.info("Time for Creating Keyword Concept Connection Table: " + 
//					(double)(end - start)/(double)1000 + "(sec)\n");
//		} catch (SQLException ex) {
//			log.warn("A warning in the process of creating keyword concept connection table:");
//			log.warn(ex.getMessage());
//		} 
//	} 
	
	private int getMaxEntityId() {
		int maxEntityId = 0;
		if (!m_dbService.hasTable(Environment.ENTITY_TABLE)) 
			return maxEntityId;
		try {
			Statement stmt = m_dbService.createStatement();
//			String sql = "select count(*) from " + Environment.ENTITY_TABLE;
			String sql = "select max(" + Environment.ENTITY_ID_COLUMN + ") " + " from " + Environment.ENTITY_TABLE;
			ResultSet rs = stmt.executeQuery(sql);
			
			if(rs.next())
				maxEntityId = rs.getInt(1);
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return maxEntityId;
	}
	
	private int getMaxDataSourceId() {
		int maxDsId = 0;
		if (!m_dbService.hasTable(Environment.DATASOURCE_TABLE)) 
			return maxDsId;
		try {
			Statement stmt = m_dbService.createStatement();
			String sql = "select max(" + Environment.DATASOURCE_ID_COLUMN + ") " + 
				" from " + Environment.DATASOURCE_TABLE;
			ResultSet rs = stmt.executeQuery(sql);
			
			if(rs.next())
				maxDsId = rs.getInt(1);
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return maxDsId;
	}
	
	private Collection<String> getAllowedDataSources() {
		String dsFile = m_config.getDsFile();
		int fromDsNum = m_config.getDsFromNum();
		int endDsNum = m_config.getDsEndNum();
		
		if(dsFile != Environment.DEFAULT_DS_FILEPATH) {
			Collection<String> dataSources = new HashSet<String>();
			BufferedReader reader;
			try {
				reader = new BufferedReader(new FileReader(dsFile));
				String line;
				int i = 1;
				while((line = reader.readLine()) != null && i <= endDsNum){
					if(i >= fromDsNum) {
						String[] str = line.trim().split("\t");
						dataSources.add(str[0].trim());
						log.debug(line);
					}
					i++;
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(dataSources.size() != 0) {
				return dataSources;
			}
			else 
				return null;
		}
		else 
			return null;
			
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
	
    private boolean isalpha(char x) {
        return (x >= 'A' && x <= 'Z') || (x >= 'a' && x <= 'z');
    }
    
    private boolean isdigit(char x) {
        return x >= '0' && x <= '9';
    }

    public boolean containsDigit(String str) {
		char[] chs = str.toCharArray();
		for (int i = 0; i < chs.length; i++) {
			if (Character.isDigit(chs[i])) {
				return true;
			}
		}
		return false;
	}
    
    public boolean containsOnlyLetters(String str) {
		char[] chs = str.toCharArray();
		for (int i = 0; i < chs.length; i++) {
			if (!isalpha(chs[i])) {
				return false;
			}
		}
		return true;
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

	
	public void memoryInfo(){
//		log.info("max mem: " + Runtime.getRuntime().maxMemory()/(1024*1024));
		log.info("total mem: " + Runtime.getRuntime().totalMemory()/(1024*1024));
		log.info("available mem: " + Runtime.getRuntime().freeMemory()/(1024*1024));
		log.info("used mem: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/(1024*1024));
	}
	
	class DbTripleSink implements TripleSink {
		PreparedStatement ps;
		
		public DbTripleSink() {
			String insertSql = "insert into " + Environment.TRIPLE_TABLE + "(" + 
				Environment.TRIPLE_SUBJECT_COLUMN + "," + 
				Environment.TRIPLE_PROPERTY_COLUMN + "," + 
				Environment.TRIPLE_OBJECT_COLUMN + "," + 
				Environment.TRIPLE_PROPERTY_TYPE + "," + 
				Environment.TRIPLE_DS_COLUMN + ") values(?, ?, ?, ?, ?)";
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
//				log.warn("A warning in the process of importing triple into triple table:");
//				log.warn(ex.getMessage());
				log.debug("A warning in the process of importing triple into triple table:");
				log.debug(ex.getMessage());
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
