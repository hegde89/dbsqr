package edu.unika.aifb.dbsqr.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

import edu.unika.aifb.dbsqr.Environment;
import edu.unika.aifb.dbsqr.importer.Importer;
import edu.unika.aifb.dbsqr.importer.NTriplesImporter;
import edu.unika.aifb.dbsqr.importer.RDFImporter;
import edu.unika.aifb.dbsqr.importer.TripleSink;
import edu.unika.aifb.dbsqr.util.KeywordTokenizer;
import edu.unika.aifb.dbsqr.util.Stemmer;

public class DsSelectIndexBuilder {
	
	private static final Logger log = Logger.getLogger(DsSelectIndexBuilder.class);
	
	private DbService m_dbService;
	private Map<Integer, Importer> m_importers;
	private Map<String, Integer> m_dataSources;
	private DbConfig m_config;
	
	
	public DsSelectIndexBuilder(DbConfig config) {
		this(config, true);
	}
	
	public DsSelectIndexBuilder(DbConfig config, boolean createDb) {
		m_config = config;
		m_importers = new HashMap<Integer, Importer>();
		m_dataSources = new HashMap<String, Integer>();
		
		initializeImporters();
		initializeDbService(createDb);	
	}
	
	public void initializeImporters() {
		Map<String,List<String>> dsAndFilePaths = m_config.getDsAndFilePaths();
		int dsId =1;
		for(String ds : dsAndFilePaths.keySet()) {
			for(String filePath : dsAndFilePaths.get(ds)) {
				File file = new File(filePath);
				List<String> subfilePaths = getAllfilePaths(file);
				for (String subfilePath : subfilePaths) {
					if (subfilePath.contains(".nt")) {
						Importer importer = m_importers.get(Environment.NTRIPLE);
						if (importer == null) {
							importer = new NTriplesImporter();
							m_importers.put(Environment.NTRIPLE, importer);
						}
						importer.addImport(ds, subfilePath);
					} else if (subfilePath.contains(".rdf")) {
						Importer importer = m_importers.get(Environment.RDFXML);
						if (importer == null) {
							importer = new RDFImporter();
							m_importers.put(Environment.RDFXML, importer);
						}
						importer.addImport(ds, subfilePath);
					} else {
						log.warn("file type unknown for data source " + ds);
					}
				}
			}
			m_dataSources.put(ds, dsId);
			dsId++;
		}
	} 
	
	public List<String> getAllfilePaths(File file) {
		ArrayList<String> subfilePaths = new ArrayList<String>();
		if (file.isDirectory()) {
			for (File subfile : file.listFiles()) {
				if(subfile.isDirectory()) {
					subfilePaths.addAll(getAllfilePaths(subfile));
				}
				else {
					if (!subfile.getName().startsWith(".")) {
						subfilePaths.add(file.getAbsolutePath());
					}	
				}
			}	
		}	
		else {
			subfilePaths.add(file.getAbsolutePath());
		}
		
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
	
	public void createDatasourceTable() {
		log.info("---- Creating Datasource Table ----");
        Statement stmt = m_dbService.createStatement();
        try {
			if (m_dbService.hasTable(Environment.DATASOURCE_TABLE)) {
				stmt.execute("drop table " + Environment.DATASOURCE_TABLE);
			}
			String createSql = "create table " + 
				Environment.DATASOURCE_TABLE + "( " + 
				Environment.DATASOURCE_ID_COLUMN + " smallint unsigned not null primary key, " + 
				Environment.DATASOURCE_NAME_COLUMN + " varchar(50) not null, " + 
				"index(" + Environment.DATASOURCE_ID_COLUMN +")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			
			log.info("---- Populating Datasource Table ----");
			String insertSql = "insert into " + Environment.DATASOURCE_TABLE + " values(?, ?)";
			PreparedStatement ps = m_dbService.createPreparedStatement(insertSql);
			for (String dsName : m_dataSources.keySet()) {
				ps.setInt(1, m_dataSources.get(dsName));
				ps.setString(2, dsName);
				ps.executeUpdate();
			}
			if(ps != null)
				ps.close();
			if(stmt != null)
				stmt.close();
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating datasource table:");
			log.warn(ex.getMessage());
		}  
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
				Environment.TRIPLE_PROPERTY_COLUMN + " varchar(100) not null, " + 
				Environment.TRIPLE_OBJECT_COLUMN + " varchar(100) not null, " + 
				Environment.TRIPLE_PROPERTY_TYPE + " tinyint(1) unsigned not null, " +
				Environment.TRIPLE_DS_ID_COLUMN + " smallint unsigned not null) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + " add index (" + Environment.TRIPLE_PROPERTY_TYPE + ")");
			stmt.execute("alter table " + Environment.TRIPLE_TABLE + " add index (" + Environment.TRIPLE_ID_COLUMN + ")");
			
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
				Environment.SCHEMA_DS_ID_COLUMN + " smallint unsigned not null, " + 
				"index(" + Environment.SCHEMA_TYPE_COLUMN + "), " + 
				"index(" + Environment.SCHEMA_URI_COLUMN + ")) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			
			log.info("---- Populating Schema Table ----");
			String insertSql = "insert into " + Environment.SCHEMA_TABLE + "(" + 
				Environment.SCHEMA_URI_COLUMN + ", " +
				Environment.SCHEMA_TYPE_COLUMN + ", " + 
				Environment.SCHEMA_DS_ID_COLUMN + ") ";
			String sql = "values('http://www.w3.org/2002/07/owl#Thing', " + 
				Environment.CONCEPT + ", " + Environment.TOP_CONCEPT + ")"; 
			stmt.executeUpdate(insertSql + sql);
		
			String selectSql = "select distinct " + 
				Environment.TRIPLE_OBJECT_COLUMN + ", " + 
				Environment.CONCEPT + ", " +
				Environment.TRIPLE_DS_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.TYPE;
			stmt.executeUpdate(insertSql + selectSql);
			
			selectSql = "select distinct " + 
				Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
				Environment.OBJECT_PROPERTY + ", " +
				Environment.TRIPLE_DS_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY;
			stmt.executeUpdate(insertSql + selectSql);
			
			selectSql = "select distinct " + 
				Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
				Environment.DATA_PROPERTY + ", " +
				Environment.TRIPLE_DS_ID_COLUMN +
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
				Environment.ENTITY_DS_ID_COLUMN + " smallint unsigned not null) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.ENTITY_TABLE + " add index (" + Environment.ENTITY_ID_COLUMN + ")");
			stmt.execute("alter table " + Environment.ENTITY_TABLE + " add index (" + Environment.ENTITY_URI_COLUMN + ")");
			
			log.info("---- Populating Entity Table ----");
			String insertSql = "insert into " + Environment.ENTITY_TABLE + "(" + 
				Environment.ENTITY_URI_COLUMN + ", " + Environment.ENTITY_DS_ID_COLUMN + ") "; 
			String selectSql = "select distinct " + 
				Environment.TRIPLE_SUBJECT_COLUMN + ", " + 
				Environment.TRIPLE_DS_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + 
				" union distinct " + "select distinct " + 
				Environment.TRIPLE_OBJECT_COLUMN + ", " + 
				Environment.TRIPLE_DS_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + 
				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + 
				Environment.OBJECT_PROPERTY;
			String insertSelectSql = insertSql + selectSql;
			stmt.executeUpdate(insertSelectSql);
			
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Entity Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity table:");
			log.warn(ex.getMessage());
		}  
	} 
	
	public void createEntityConceptMembershipTable() {
		log.info("---- Creating Entity Concept Membership Table ----");
        Statement stmt = m_dbService.createStatement();
        long start = System.currentTimeMillis();
        try {
			if (m_dbService.hasTable(Environment.ENTITY_CONCEPT_MEMBERSHIP_TABLE)) {
				stmt.execute("drop table " + Environment.ENTITY_CONCEPT_MEMBERSHIP_TABLE);
			}
			String createSql = "create table " + 
				Environment.ENTITY_CONCEPT_MEMBERSHIP_TABLE + "( " + 
				Environment.MEMBERSHIP_ENTITY_ID_COLUMN + " int unsigned not null, " + 
				Environment.MEMBERSHIP_CONCEPT_ID_COLUMN + " mediumint unsigned not null) " +
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Environment.ENTITY_CONCEPT_MEMBERSHIP_TABLE + " add index (" + Environment.MEMBERSHIP_ENTITY_ID_COLUMN + ")");
			
			log.info("---- Populating Entity Table ----");
			String insertSql = "insert into " + Environment.ENTITY_CONCEPT_MEMBERSHIP_TABLE + "(" + 
				Environment.MEMBERSHIP_ENTITY_ID_COLUMN + ", " + Environment.MEMBERSHIP_CONCEPT_ID_COLUMN + ") "; 
			String selectSql = "select distinct " + 
				"C." + Environment.ENTITY_ID_COLUMN + ", B." + Environment.SCHEMA_ID_COLUMN +
				" from " + Environment.TRIPLE_TABLE + " as A, " + Environment.SCHEMA_TABLE + " as B, " + Environment.ENTITY_TABLE + " as C " +
				" where " + "A." + Environment.TRIPLE_SUBJECT_COLUMN + " = " + "C." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_OBJECT_COLUMN + " = " + "B." + Environment.SCHEMA_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.TYPE + 
				" and " + "B." + Environment.SCHEMA_TYPE_COLUMN + " = " + Environment.CONCEPT; 
			String insertSelectSql = insertSql + selectSql;
			stmt.executeUpdate(insertSelectSql);
			
			if(stmt != null)
				stmt.close();
			
			long end = System.currentTimeMillis();
			log.info("Time for Creating Entity Concept Membership Table: " + (double)(end - start)/(double)1000 + "(sec)");
		} catch (SQLException ex) {
			log.warn("A warning in the process of creating entity concept membership table:");
			log.warn(ex.getMessage());
		}  
	}
	
	public void createEntityRelationTable() {
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
				"primary key("	+ Environment.ENTITY_RELATION_UID_COLUMN + ", " + Environment.ENTITY_RELATION_VID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + entityRelationtTable_1 + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")");
			stmt.execute("alter table " + entityRelationtTable_1 + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")");
			
			log.info("---- Populating Entity Relation Table ----");
			// Populate Temporal Entity Relation Table 
			String insertSql = "insert IGNORE into " + entityRelationtTable_1 + " values(?, ?)"; 
			PreparedStatement ps = m_dbService.createPreparedStatement(insertSql,ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
			String selectSql = "select " + "B." + Environment.ENTITY_ID_COLUMN + ", " + "C." + Environment.ENTITY_ID_COLUMN + 
				" from " + Environment.ENTITY_TABLE + " as B, " + Environment.ENTITY_TABLE + " as C, " +
				Environment.TRIPLE_TABLE + " as A " + 
				" where " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
				" and " + "A." + Environment.TRIPLE_SUBJECT_COLUMN + " = " + "B." + Environment.ENTITY_URI_COLUMN + 
				" and " + "A." + Environment.TRIPLE_OBJECT_COLUMN + " = " + "C." + Environment.ENTITY_URI_COLUMN + 
				" group by " + "B." + Environment.ENTITY_ID_COLUMN + ", " + "C." + Environment.ENTITY_ID_COLUMN; 
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
	
	public void createEntityRelationTable(int distance) {
		log.info("---- Creating Entity Relation Table at distance " + distance + " ----");
		long start = System.currentTimeMillis();
        Statement stmt = m_dbService.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        String Rd = Environment.ENTITY_RELATION_TABLE + distance; 
        try {
        	// Create Entity Relation Table at distance d
			if (m_dbService.hasTable(Rd)) {
				stmt.execute("drop table " + Rd);
			}
			String createSql = "create table " + Rd + "( " + 
				Environment.ENTITY_RELATION_UID_COLUMN + " int unsigned not null, " + 
				Environment.ENTITY_RELATION_VID_COLUMN + " int unsigned not null, " +
				"primary key("	+ Environment.ENTITY_RELATION_UID_COLUMN + ", " + Environment.ENTITY_RELATION_VID_COLUMN + ")) " + 
				"ENGINE=MyISAM";
			stmt.execute(createSql);
			stmt.execute("alter table " + Rd + " add index (" + Environment.ENTITY_RELATION_UID_COLUMN + ")");
			stmt.execute("alter table " + Rd + " add index (" + Environment.ENTITY_RELATION_VID_COLUMN + ")");
			
			log.info("---- Populating Entity Relation Table at distance " + distance + " ----");
			// Populate Temporal Entity Relation Table at distance d
			int num = 0;
			if(distance == 2){
				String insertSql = "insert IGNORE into " + Rd + " values(?, ?)"; 
				PreparedStatement ps = m_dbService.createPreparedStatement(insertSql, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
				String R1 = Environment.ENTITY_RELATION_TABLE + 1;
				String selectSql = 	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN +
					" from " + R1 + " as A, " + R1 + " as B " +
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
				
				selectSql =	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_UID_COLUMN +
					" from " + R1 + " as A, " + R1 + " as B " +
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
				
				selectSql =	"select " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN +
					" from " + R1 + " as A, " + R1 + " as B " +
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
					String Ri = Environment.ENTITY_RELATION_TABLE + i;
					String deleteSql = "delete " + Rd + " from " + Rd + ", " + Ri +
						" where " + Rd + "." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
						Ri + "." + Environment.ENTITY_RELATION_UID_COLUMN + " and " + 
						Rd + "." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
						Ri + "." + Environment.ENTITY_RELATION_VID_COLUMN;
					deletedRows += stmt.executeUpdate(deleteSql);
				}
				log.info("Number of duplicated rows that are deleted: " + deletedRows);
			}
			
			if(distance >= 3){	
				String insertSql = "insert IGNORE into " + Rd + " values(?, ?)"; 
				PreparedStatement ps = m_dbService.createPreparedStatement(insertSql);
				String R1 = Environment.ENTITY_RELATION_TABLE + 1;
				String R = Environment.ENTITY_RELATION_TABLE + (distance - 1);
				String selectSql = 	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN + 
					" from " + R + " as A, " + R1 + " as B " +
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
				
				selectSql = "select " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + 
					" from " + R + " as A, " + R1 + " as B " +
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
				
				selectSql =	"select " + "A." + Environment.ENTITY_RELATION_UID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_UID_COLUMN + 
					" from " + R + " as A, " + R1 + " as B " +
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
				
				selectSql =	"select " + "A." + Environment.ENTITY_RELATION_VID_COLUMN + ", " + "B." + Environment.ENTITY_RELATION_VID_COLUMN + ", " +
					" from " + R + " as A, " + R1 + " as B " +
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
					String Ri = Environment.ENTITY_RELATION_TABLE + i;
					String deleteSql = "delete " + Rd + " from " + Rd + ", " + Ri +
						" where " + Rd + "." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
						Ri + "." + Environment.ENTITY_RELATION_UID_COLUMN + " and " + 
						Rd + "." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
						Ri + "." + Environment.ENTITY_RELATION_VID_COLUMN;
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
	
	public void createKeywordEntityInclusionTable() {
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
				Environment.KEYWORD_ENTITY_INCLUSION_DATASOUCE_ID_COLUMN + " smallint unsigned not null, " + 
				"primary key(" + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ", " +
				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_TYPE_COLUMN + ", " +
				Environment.KEYWORD_ENTITY_INCLUSION_DATASOUCE_ID_COLUMN + ")) " + 
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
			String insertKeywEntitySql = "insert into " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " values(?, ?, ?, ?, ?)"; 
			PreparedStatement psInsertKeywEntity = m_dbService.createPreparedStatement(insertKeywEntitySql, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
			
			// Statement for Entity Table
			String selectEntitySql = "select " + Environment.ENTITY_ID_COLUMN + 
				" from " + Environment.ENTITY_TABLE +
				" where " + Environment.ENTITY_URI_COLUMN + " = ?";
			PreparedStatement psQueryEntity = m_dbService.createPreparedStatement(selectEntitySql, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
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
				int dsId = rsTriple.getInt(Environment.TRIPLE_DS_ID_COLUMN);
				String subject = rsTriple.getString(Environment.TRIPLE_SUBJECT_COLUMN);
				String property = rsTriple.getString(Environment.TRIPLE_PROPERTY_COLUMN);
				String object = rsTriple.getString(Environment.TRIPLE_OBJECT_COLUMN);
				if(propertyType == Environment.DATA_PROPERTY) {
					psQueryEntity.setString(1, subject);
					rsEntity = psQueryEntity.executeQuery();
					rsEntity.next();
					int subjectId = rsEntity.getInt(Environment.ENTITY_ID_COLUMN);
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
							psInsertKeywEntity.setFloat(4, Environment.KEYWORD_OF_DATA_VALUE);
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
				else if(propertyType == Environment.TYPE) {
					psQueryEntity.setString(1, subject);
					rsEntity = psQueryEntity.executeQuery();
					rsEntity.next();
					int subjectId = rsEntity.getInt(Environment.ENTITY_ID_COLUMN);
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
				Environment.TRIPLE_DS_ID_COLUMN +") values(?, ?, ?, ?, ?)";
			ps = m_dbService.createPreparedStatement(insertSql);
		}  
		
		public void triple(String subject, String property, String object, int type, String ds) {
			try {
				ps.setString(1, subject);
				ps.setString(2, property);
				ps.setString(3, object);
				ps.setInt(4, type);
				ps.setInt(5, m_dataSources.get(ds));
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
