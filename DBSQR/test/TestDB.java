import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.dbsqr.Environment;
import edu.unika.aifb.dbsqr.util.KeywordTokenizer;
import edu.unika.aifb.dbsqr.util.Stemmer;

public class TestDB {

	private static final Logger log = Logger.getLogger(TestDB.class);
	
	private static final String[] dataSources = {"ds1", "ds2", "ds3", "ds4", "ds5"};
	private static final Object[][] data = {{"sub1", "dprop1", "lit11 lit12", Environment.DATA_PROPERTY, 1}, 
											{"sub2", "type", "con2", Environment.ENTITY_MEMBERSHIP_PROPERTY, 4},
											{"sub2", "type", "con3", Environment.ENTITY_MEMBERSHIP_PROPERTY, 4},
											{"sub2", "type", "con5", Environment.ENTITY_MEMBERSHIP_PROPERTY, 4},
											{"sub3", "oprop3", "obj3", Environment.OBJECT_PROPERTY, 3}, 
											{"sub4", "oprop4", "obj4", Environment.OBJECT_PROPERTY, 3},
											{"sub5", "dprop5", "lit11 lit51", Environment.DATA_PROPERTY, 2}, 
											{"sub6", "type", "con6", Environment.ENTITY_MEMBERSHIP_PROPERTY, 1},
											{"sub6", "type", "con7", Environment.ENTITY_MEMBERSHIP_PROPERTY, 1},
											{"sub7", "con7", "obj8", Environment.OBJECT_PROPERTY, 3}};
	
	
	public static void main(String[] args) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/dbsqr", "root", "root");
			Statement stmt = conn.createStatement();
			
			String sql = "select * from test";
			
			ResultSet rs =  stmt.executeQuery(sql);
			while(rs.next()) {
				int id = rs.getInt("id");
				int cid = rs.getInt("cid");
				String concept = rs.getString("concept");
				if(cid == 0)
					log.info("cid = 0");
				if(concept == null)
					log.info("concept = null");
				log.info(id + " | " + cid + " | " + concept);
			} 
			
			
		} catch(SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
	} 
	
	
	/**
	 * @param args
	 */
//	public static void main(String[] args) {
//		Connection conn = null;
//		try {
//			Class.forName("com.mysql.jdbc.Driver");
//			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
//			Statement stmt = conn.createStatement();
//			
//			DatabaseMetaData meta = conn.getMetaData();
//			ResultSet rs = meta.getTables(null, null, null, null);
//			Set<String> tables = new HashSet<String>(); 
//			while(rs.next()) {
//				tables.add(rs.getString(3));
//			}
//			rs.close();
//			
//			long start = System.currentTimeMillis();
//			
//			////////////////////////////////////////////////////////////////////////////////////////
//			log.info("---- Creating Datasource Table ----");
//			if(tables.contains(Environment.DATASOURCE_TABLE)) {
//				stmt.executeUpdate("drop table " + Environment.DATASOURCE_TABLE); 
//			}
//			String createSql = "create table " + 
//				Environment.DATASOURCE_TABLE + "( " + 
//				Environment.DATASOURCE_ID_COLUMN + " smallint unsigned not null primary key, " + 
//				Environment.DATASOURCE_NAME_COLUMN + " varchar(50) not null) " + 
//				"ENGINE=MyISAM";
//			stmt.execute(createSql);
//			
//			log.info("---- Populating Datasource Table ----");
//			String insertSql = "insert into " + Environment.DATASOURCE_TABLE + " values(?, ? )";
//			PreparedStatement ps = conn.prepareStatement(insertSql);
//			int dsId = 0;
//			for (String dsName : dataSources) {
//				ps.setInt(1, dsId);
//				dsId++;
//				ps.setString(2, dsName);
//				ps.executeUpdate();
//			}
//			ps.close();
//			
//			
//			////////////////////////////////////////////////////////////////////////////////////////
//			log.info("---- Creating Triple Table ----");
//			if(tables.contains(Environment.TRIPLE_TABLE)) {
//				stmt.executeUpdate("drop table " + Environment.TRIPLE_TABLE); 
//			}
//			createSql  = "create table " + Environment.TRIPLE_TABLE + "( " + 
//				Environment.TRIPLE_ID_COLUMN + " int unsigned not null primary key auto_increment, " + 
//				Environment.TRIPLE_SUBJECT_COLUMN + " varchar(100) not null, " + 
//				Environment.TRIPLE_PROPERTY_COLUMN + " varchar(100) not null, " + 
//				Environment.TRIPLE_OBJECT_COLUMN + " varchar(100) not null, " + 
//				Environment.TRIPLE_PROPERTY_TYPE + " tinyint(1) unsigned not null, " +
//				Environment.TRIPLE_DS_COLUMN + " smallint unsigned not null) " +
//				"ENGINE=MyISAM";
//			stmt.execute(createSql);
//			
//			log.info("---- Importing Triples into Triple Table ----");
//			insertSql = "insert into " + Environment.TRIPLE_TABLE + "(" + 
//				Environment.TRIPLE_SUBJECT_COLUMN +"," + 
//				Environment.TRIPLE_PROPERTY_COLUMN +"," + 
//				Environment.TRIPLE_OBJECT_COLUMN +"," + 
//				Environment.TRIPLE_PROPERTY_TYPE +"," + 
//				Environment.TRIPLE_DS_COLUMN +") values(?, ?, ?, ?, ?)";
//			ps = conn.prepareStatement(insertSql);
//			for(Object[] row : data) {
//				for(int i = 0; i < 5; i++) {
//					if(row[i] instanceof String) 
//						ps.setString(i+1, (String)row[i]);
//					else if(row[i] instanceof Integer)
//						ps.setInt(i+1, (Integer)row[i]);
//				}
//				ps.executeUpdate();
//			}
//
//			
//			////////////////////////////////////////////////////////////////////////////////////////
//			log.info("---- Creating Schema Table ----");
//			if(tables.contains(Environment.SCHEMA_TABLE)) {
//				stmt.executeUpdate("drop table " + Environment.SCHEMA_TABLE); 
//			}
//			createSql = "create table " + 
//				Environment.SCHEMA_TABLE + "( " + 
//				Environment.SCHEMA_ID_COLUMN + " mediumint unsigned not null primary key auto_increment, " + 
//				Environment.SCHEMA_URI_COLUMN + " varchar(100) not null, " + 
//				Environment.SCHEMA_TYPE_COLUMN + " tinyint(1) unsigned not null, " +
//				Environment.SCHEMA_DS_ID_COLUMN + " smallint unsigned not null) " +
//				"ENGINE=MyISAM";
//			stmt.execute(createSql);
//		
//			log.info("---- Populating Schema Table ----");
//			insertSql = "insert into " + Environment.SCHEMA_TABLE + "(" + 
//				Environment.SCHEMA_URI_COLUMN + ", " +
//				Environment.SCHEMA_TYPE_COLUMN + ", " + 
//				Environment.SCHEMA_DS_ID_COLUMN + ") ";
//			String sql = "values('http://www.w3.org/2002/07/owl#Thing', " + 
//				Environment.CONCEPT + ", " + Environment.TOP_CONCEPT + ")"; 
//			stmt.executeUpdate(insertSql + sql);
//		
//			String selectSql = "select distinct " + 
//				Environment.TRIPLE_OBJECT_COLUMN + ", " + 
//				Environment.CONCEPT + ", " +
//				Environment.TRIPLE_DS_COLUMN +
//				" from " + Environment.TRIPLE_TABLE + 
//				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.ENTITY_MEMBERSHIP_PROPERTY;
//			stmt.executeUpdate(insertSql + selectSql);
//			
//			selectSql = "select distinct " + 
//				Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
//				Environment.OBJECT_PROPERTY + ", " +
//				Environment.TRIPLE_DS_COLUMN +
//				" from " + Environment.TRIPLE_TABLE + 
//				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY;
//			stmt.executeUpdate(insertSql + selectSql);
//			
//			selectSql = "select distinct " + 
//				Environment.TRIPLE_PROPERTY_COLUMN + ", " + 
//				Environment.DATA_PROPERTY + ", " +
//				Environment.TRIPLE_DS_COLUMN +
//				" from " + Environment.TRIPLE_TABLE + 
//				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.DATA_PROPERTY;
//			stmt.executeUpdate(insertSql + selectSql);
//			
//			
//			////////////////////////////////////////////////////////////////////////////////////////
//			log.info("---- Creating Entity Table ----");
//			if(tables.contains(Environment.ENTITY_TABLE)) {
//				stmt.executeUpdate("drop table " + Environment.ENTITY_TABLE); 
//			}
//			createSql = "create table " + 
//				Environment.ENTITY_TABLE + "( " + 
//				Environment.ENTITY_ID_COLUMN + " int unsigned not null primary key auto_increment, " + 
//				Environment.ENTITY_URI_COLUMN + " varchar(100) not null, " + 
//				Environment.ENTITY_CONCEPT_ID_COLUMN + " varchar(100) not null " + 
//				"default " + "'" + Environment.TOP_CONCEPT + "'" + ", " +
//				Environment.ENTITY_DS_ID_COLUMN + " smallint unsigned not null) " +
//				"ENGINE=MyISAM";
//			stmt.execute(createSql);
//			
//			log.info("---- Populating Entity Table ----");
//			insertSql = "insert into " + Environment.ENTITY_TABLE + "(" + 
//				Environment.ENTITY_URI_COLUMN + ", " + 
//				Environment.ENTITY_DS_ID_COLUMN + ") "; 
//			selectSql = "select distinct " + 
//				Environment.TRIPLE_SUBJECT_COLUMN + ", " + 
//				Environment.TRIPLE_DS_COLUMN +
//				" from " + Environment.TRIPLE_TABLE + 
//				" union distinct " + " select distinct " + 
//				Environment.TRIPLE_OBJECT_COLUMN + ", " + 
//				Environment.TRIPLE_DS_COLUMN +
//				" from " + Environment.TRIPLE_TABLE + 
//				" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + 
//				Environment.OBJECT_PROPERTY;
//			stmt.executeUpdate(insertSql + selectSql);
//			
//			selectSql = "select " + Environment.ENTITY_ID_COLUMN + "," + 
//				" group_concat(distinct " + Environment.SCHEMA_TABLE + "." + Environment.SCHEMA_ID_COLUMN + 
//				" order by " + Environment.SCHEMA_TABLE + "." + Environment.SCHEMA_ID_COLUMN + 
//				" separator '_'" + ") " + 
//				" from " + Environment.TRIPLE_TABLE + ", " + Environment.SCHEMA_TABLE + ", " + Environment.ENTITY_TABLE +
//				" where " + Environment.TRIPLE_TABLE + "." + Environment.TRIPLE_SUBJECT_COLUMN + " = " +
//				Environment.ENTITY_TABLE + "." + Environment.ENTITY_URI_COLUMN + 
//				" and " + Environment.TRIPLE_TABLE + "." + Environment.TRIPLE_OBJECT_COLUMN + " = " + 
//				Environment.SCHEMA_TABLE + "." + Environment.SCHEMA_URI_COLUMN + 
//				" and " + Environment.TRIPLE_TABLE + "." + Environment.TRIPLE_PROPERTY_TYPE + " = " + 
//				Environment.ENTITY_MEMBERSHIP_PROPERTY + 
//				" and " + Environment.SCHEMA_TABLE + "." + Environment.SCHEMA_TYPE_COLUMN + " = " + 
//				Environment.CONCEPT + 
//				" group by " + Environment.TRIPLE_TABLE + "." + Environment.TRIPLE_SUBJECT_COLUMN;  
//			rs = stmt.executeQuery(selectSql);
//			String updateSql =  "update " + Environment.ENTITY_TABLE + ", " + 
//				Environment.TRIPLE_TABLE + ", " + Environment.SCHEMA_TABLE +
//				" set " + Environment.ENTITY_CONCEPT_ID_COLUMN + " = ? " +
//				" where " + Environment.ENTITY_ID_COLUMN + " = ?";
//			ps = conn.prepareStatement(updateSql);
//			while(rs.next()) {
//				ps.setString(1, rs.getString(2));
//				ps.setInt(2, rs.getInt(1));
//				ps.executeUpdate();
//			}
//			rs.close();
//			ps.close();
//			
//		
//			////////////////////////////////////////////////////////////////////////////////////////
//			log.info("---- Creating Entity Relation Table ----");
//	        String entityRelationtTable_1 = Environment.ENTITY_RELATION_TABLE + 1; 
//	        if(tables.contains(entityRelationtTable_1)) {
//				stmt.executeUpdate("drop table " + entityRelationtTable_1); 
//			}
//			createSql = "create table " + entityRelationtTable_1 + "( " + 
//				Environment.ENTITY_RELATION_UID_COLUMN + " int unsigned not null, " + 
//				Environment.ENTITY_RELATION_VID_COLUMN + " int unsigned not null, " + 
//				Environment.ENTITY_RELATION_PATH_COLUMN + " varchar(100) not null, " +
//				"primary key(" + Environment.ENTITY_RELATION_UID_COLUMN + ", " + 
//				Environment.ENTITY_RELATION_VID_COLUMN + ", " +
//				Environment.ENTITY_RELATION_PATH_COLUMN + ")) " + 
//				"ENGINE=MyISAM";
//			stmt.execute(createSql);
//				
//			log.info("---- Populating Entity Relation Table ----");
//			insertSql = "insert into " + entityRelationtTable_1 + " values(?, ?, ?)"; 
//			ps = conn.prepareStatement(insertSql);
//			selectSql = "select " + 
//				"B." + Environment.ENTITY_ID_COLUMN + ", " + 
//				"C." + Environment.ENTITY_ID_COLUMN + ", " +
//				"A." + Environment.TRIPLE_ID_COLUMN + 
//				" from " + Environment.TRIPLE_TABLE + " as A, " + 
//				Environment.ENTITY_TABLE + " as B, " + 
//				Environment.ENTITY_TABLE + " as C " + 
//				" where " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + 
//				Environment.OBJECT_PROPERTY + 
//				" and " + "A." + Environment.TRIPLE_SUBJECT_COLUMN + " = " + 
//				"B." + Environment.ENTITY_URI_COLUMN + 
//				" and " + "A." + Environment.TRIPLE_OBJECT_COLUMN + " = " + 
//				"C." + Environment.ENTITY_URI_COLUMN;
//			rs = stmt.executeQuery(selectSql);
//	        while (rs.next()){
//	           	int entityId1 = rs.getInt(1);
//	           	int entityId2 = rs.getInt(2);
//	           	int tripleId = rs.getInt(3);
//	           	if(entityId1 < entityId2){
//	           		ps.setInt(1, entityId1);
//	           		ps.setInt(2, entityId2);
//	           	}else{
//	           		ps.setInt(1, entityId2);
//	           		ps.setInt(2, entityId1);
//	           	}
//	           	ps.setString(3, String.valueOf(tripleId));
//	            ps.executeUpdate();
//	        }
//	        rs.close();
//			ps.close();
//			
//			
//			////////////////////////////////////////////////////////////////////////////////////////
//			log.info("---- Creating Keyword Entity Inclusion Table and Keyword Table ----");
//			if (tables.contains(Environment.KEYWORD_TABLE)) {
//				stmt.execute("drop table " + Environment.KEYWORD_TABLE);
//			}
//			createSql = "create table " + Environment.KEYWORD_TABLE + "( " + 
//				Environment.KEYWORD_ID_COLUMN + " int unsigned not null primary key, " + 
//				Environment.KEYWORD_COLUMN + " varchar(30) not null) " + 
//				"ENGINE=MyISAM";
//			stmt.execute(createSql);
//			
//			if (tables.contains(Environment.KEYWORD_ENTITY_INCLUSION_TABLE)) {
//				stmt.execute("drop table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE);
//			}
//			createSql = "create table " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + "( " + 
//				Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + " int unsigned not null, " + 
//				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + " int unsigned not null, " + 
//				Environment.KEYWORD_ENTITY_INCLUSION_SCORE_COLUMN + " float unsigned not null, " + 
//				Environment.KEYWORD_ENTITY_INCLUSION_PATH_COLUMN + " varchar(20) not null, " + 
//				"primary key(" + Environment.KEYWORD_ENTITY_INCLUSION_KEYWORD_ID_COLUMN + ", " + 
//				Environment.KEYWORD_ENTITY_INCLUSION_ENTITY_ID_COLUMN + ", " +
//				Environment.KEYWORD_ENTITY_INCLUSION_PATH_COLUMN + ")) " + 
//				"ENGINE=MyISAM";
//			stmt.execute(createSql);
//			
//			log.info("---- Populating Keyword Entity Inclusion Table and Keyword Table ----");
//			HashSet<String> stopWords = new HashSet<String>();
//			Stemmer stemmer = new Stemmer();
//			
//			// Statement for Keyword Table
//			String insertKeywTableSql = "insert into " + Environment.KEYWORD_TABLE + " values(?, ?)"; 
//			PreparedStatement psInsertKeywTable = conn.prepareStatement(insertKeywTableSql);
//			String selectKeywTableSql = "select " + Environment.KEYWORD_ID_COLUMN +  
//				" from " + Environment.KEYWORD_TABLE +
//				" where " + Environment.KEYWORD_COLUMN + " = ?";
//			PreparedStatement psQueryKeywTable = conn.prepareStatement(selectKeywTableSql);
//			ResultSet rsKeywordTable = null;
//			
//			// Statement for Keyword Entity Inclusion Table
//			String insertKeywEntityTableSql = "insert into " + Environment.KEYWORD_ENTITY_INCLUSION_TABLE + " values(?, ?, ?, ?)"; 
//			PreparedStatement psInsertKeywEntityTable = conn.prepareStatement(insertKeywEntityTableSql);
//			
//			// Statement for Entity Table
//			String selectEntityTableSql = "select " + Environment.ENTITY_ID_COLUMN + ", " + Environment.ENTITY_URI_COLUMN + 
//				" from " + Environment.ENTITY_TABLE;
//			ResultSet rsEntityTable = stmt.executeQuery(selectEntityTableSql);
//			
//			// Statement for Triple Table
//			String selectTripleTableSqlFw = "select " + Environment.TRIPLE_ID_COLUMN + ", " + Environment.TRIPLE_PROPERTY_TYPE + ", " +
//				Environment.TRIPLE_PROPERTY_COLUMN + ", " + Environment.TRIPLE_OBJECT_COLUMN + 
//				" from " + Environment.TRIPLE_TABLE +
//				" where " + Environment.TRIPLE_SUBJECT_COLUMN + " = ?";
//			PreparedStatement psQueryTripleTableFw = conn.prepareStatement(selectTripleTableSqlFw);
//			String selectTripleTableSqlBw = "select " + Environment.TRIPLE_ID_COLUMN + ", " + Environment.TRIPLE_PROPERTY_COLUMN +  
//				" from " + Environment.TRIPLE_TABLE +
//				" where " + Environment.TRIPLE_OBJECT_COLUMN + " = ? " + 
//				" and " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY;
//			PreparedStatement psQueryTripleTableBw = conn.prepareStatement(selectTripleTableSqlBw);
//			ResultSet rsTripleTable = null;
//			
//			int keywordSize = 0;
//			while(rsEntityTable.next()) {
//				String entityUri = rsEntityTable.getString(Environment.ENTITY_URI_COLUMN);
//				int entityId = rsEntityTable.getInt(Environment.ENTITY_ID_COLUMN);
//				psQueryTripleTableFw.setString(1, entityUri);
//				rsTripleTable = psQueryTripleTableFw.executeQuery();
//				while (rsTripleTable.next()) {
//					int type = rsTripleTable.getInt(Environment.TRIPLE_PROPERTY_TYPE);
//					int tripleId = rsTripleTable.getInt(Environment.TRIPLE_ID_COLUMN);
//					if(type == Environment.DATA_PROPERTY) {
//						// processing keyword for data property
//						String keywordOfDataProperty = trucateUri(rsTripleTable.getString(Environment.TRIPLE_PROPERTY_COLUMN)).toLowerCase();
//						//stem the keyword
//						stemmer.addWord(keywordOfDataProperty);
//        				stemmer.stem();
//        				keywordOfDataProperty = stemmer.toString();
//						psQueryKeywTable.setString(1, keywordOfDataProperty);
//						rsKeywordTable = psQueryKeywTable.executeQuery();
//						int keywordId;
//						if(rsKeywordTable.next()) {
//							keywordId = rsKeywordTable.getInt(Environment.KEYWORD_ID_COLUMN);
//						}
//						else {
//							keywordId = ++keywordSize;
//							psInsertKeywTable.setInt(1, keywordId);
//							psInsertKeywTable.setString(2, keywordOfDataProperty);
//							psInsertKeywTable.executeUpdate();
//						}
//						if(rsKeywordTable != null)
//							rsKeywordTable.close();
//						psInsertKeywEntityTable.setInt(1, keywordId);
//						psInsertKeywEntityTable.setInt(2, entityId);
//						psInsertKeywEntityTable.setFloat(3, Environment.BOOST_KEYWORD_OF_DATA_PROPERTY);
//						psInsertKeywEntityTable.setString(4, String.valueOf(tripleId));
//						psInsertKeywEntityTable.executeUpdate();
//						
//						// processing keywords for data property value
//						HashMap<String, Float> keywords = new HashMap<String, Float>(); 
//						String dataValue = rsTripleTable.getString(Environment.TRIPLE_OBJECT_COLUMN);
//						KeywordTokenizer tokens = new KeywordTokenizer(dataValue, stopWords);
//						List<String> terms = tokens.getAllTerms();
//						int termSize = terms.size();
//						if(termSize <= 10) {
//							for(String term : terms) {
//								//stem the keyword
//	                            stemmer.addWord(term);
//	            				stemmer.stem();
//	            				term = stemmer.toString();
//	            				//count the number of occurrences of a keyword in data property value of an entity
//	            				if (keywords.containsKey(term)){
//	            					float value = keywords.get(term) + 1.0f;
//	            					keywords.put(term, value);
//	            				}
//	            				else
//	            					keywords.put(term, 1.0f);
//							}
//							for(String keyword : keywords.keySet()) {
//								psQueryKeywTable.setString(1, keyword);
//								rsKeywordTable = psQueryKeywTable.executeQuery();
//								if(rsKeywordTable.next()) {
//									keywordId = rsKeywordTable.getInt(Environment.KEYWORD_ID_COLUMN);
//								}
//								else {
//									keywordId = ++keywordSize;
//									psInsertKeywTable.setInt(1, keywordId);
//									psInsertKeywTable.setString(2, keyword);
//									psInsertKeywTable.executeUpdate();
//								}
//								if(rsKeywordTable != null)
//									rsKeywordTable.close();
//								psInsertKeywEntityTable.setInt(1, keywordId);
//								psInsertKeywEntityTable.setInt(2, entityId);
//								psInsertKeywEntityTable.setFloat(3, Environment.BOOST_KEYWORD_OF_DATA_VALUE*(keywords.get(keyword)/termSize));
//								psInsertKeywEntityTable.setString(4, String.valueOf(tripleId));
//								psInsertKeywEntityTable.executeUpdate();
//							}
//						}  
//					}
//					else if(type == Environment.OBJECT_PROPERTY) {
//						String keywordOfObjectProperty = trucateUri(rsTripleTable.getString(Environment.TRIPLE_PROPERTY_COLUMN)).toLowerCase();
//						//stem the keyword
//						stemmer.addWord(keywordOfObjectProperty);
//        				stemmer.stem();
//        				keywordOfObjectProperty = stemmer.toString();
//						psQueryKeywTable.setString(1, keywordOfObjectProperty);
//						rsKeywordTable = psQueryKeywTable.executeQuery();
//						int keywordId;
//						if(rsKeywordTable.next()) {
//							keywordId = rsKeywordTable.getInt(Environment.KEYWORD_ID_COLUMN);
//						}
//						else {
//							keywordId = ++keywordSize;
//							psInsertKeywTable.setInt(1, keywordId);
//							psInsertKeywTable.setString(2, keywordOfObjectProperty);
//							psInsertKeywTable.executeUpdate();
//						}
//						if(rsKeywordTable != null)
//							rsKeywordTable.close();
//						psInsertKeywEntityTable.setInt(1, keywordId);
//						psInsertKeywEntityTable.setInt(2, entityId);
//						psInsertKeywEntityTable.setFloat(3, Environment.BOOST_KEYWORD_OF_OBJECT_PROPERTY);
//						psInsertKeywEntityTable.setString(4, String.valueOf(tripleId));
//						psInsertKeywEntityTable.executeUpdate();
//					}
//					else if(type == Environment.ENTITY_MEMBERSHIP_PROPERTY) {
//						String keywordOfConcept = trucateUri(rsTripleTable.getString(Environment.TRIPLE_OBJECT_COLUMN)).toLowerCase();
//						//stem the keyword
//						stemmer.addWord(keywordOfConcept);
//        				stemmer.stem();
//        				keywordOfConcept = stemmer.toString();
//						psQueryKeywTable.setString(1, keywordOfConcept);
//						rsKeywordTable = psQueryKeywTable.executeQuery();
//						int keywordId;
//						if(rsKeywordTable.next()) {
//							keywordId = rsKeywordTable.getInt(Environment.KEYWORD_ID_COLUMN);
//						}
//						else {
//							keywordId = ++keywordSize;
//							psInsertKeywTable.setInt(1, keywordId);
//							psInsertKeywTable.setString(2, keywordOfConcept);
//							psInsertKeywTable.executeUpdate();
//						}
//						if(rsKeywordTable != null)
//							rsKeywordTable.close();
//						psInsertKeywEntityTable.setInt(1, keywordId);
//						psInsertKeywEntityTable.setInt(2, entityId);
//						psInsertKeywEntityTable.setFloat(3, Environment.BOOST_KEYWORD_OF_CONCEPT);
//						psInsertKeywEntityTable.setString(4, String.valueOf(tripleId));
//						psInsertKeywEntityTable.executeUpdate();
//					}
//				}
//				if(rsTripleTable != null)
//					rsTripleTable.close();
//				
//				psQueryTripleTableBw.setString(1, entityUri);
//				rsTripleTable = psQueryTripleTableBw.executeQuery();
//				while (rsTripleTable.next()) {
//					int tripleId = rsTripleTable.getInt(Environment.TRIPLE_ID_COLUMN);
//					String keywordOfObjectProperty = trucateUri(rsTripleTable.getString(Environment.TRIPLE_PROPERTY_COLUMN)).toLowerCase();
//					//stem the keyword
//					stemmer.addWord(keywordOfObjectProperty);
//        			stemmer.stem();
//        			keywordOfObjectProperty = stemmer.toString();
//					psQueryKeywTable.setString(1, keywordOfObjectProperty);
//					rsKeywordTable = psQueryKeywTable.executeQuery();
//					int keywordId;
//					if(rsKeywordTable.next()) {
//						keywordId = rsKeywordTable.getInt(Environment.KEYWORD_ID_COLUMN);
//					}
//					else {
//						keywordId = ++keywordSize;
//						psInsertKeywTable.setInt(1, keywordId);
//						psInsertKeywTable.setString(2, keywordOfObjectProperty);
//						psInsertKeywTable.executeUpdate();
//					}
//					if(rsKeywordTable != null)
//						rsKeywordTable.close();
//					psInsertKeywEntityTable.setInt(1, keywordId);
//					psInsertKeywEntityTable.setInt(2, entityId);
//					psInsertKeywEntityTable.setFloat(3, Environment.BOOST_KEYWORD_OF_OBJECT_PROPERTY);
//					psInsertKeywEntityTable.setString(4, String.valueOf(tripleId));
//					psInsertKeywEntityTable.executeUpdate();
//				}
//				if(rsTripleTable != null)
//					rsTripleTable.close();
//			}
//			
//			if(rsEntityTable != null)
//				rsEntityTable.close();
//			if(rsKeywordTable != null)
//				rsKeywordTable.close();
//			if(psInsertKeywTable != null)
//				psInsertKeywTable.close();
//			if(psInsertKeywEntityTable != null)
//				psInsertKeywEntityTable.close();
//			if(psQueryKeywTable != null)
//				psQueryKeywTable.close();
//			if(psQueryTripleTableFw != null)
//				psQueryTripleTableFw.close();
//			if(psQueryTripleTableBw != null)
//				psQueryTripleTableBw.close();
//			
//			if(stmt != null)
//				stmt.close();
//	        
//	        long end = System.currentTimeMillis();
//	        log.info("Used Time: " + (end - start) + "(ms)");
//		} catch (ClassNotFoundException e) {
//			log.warn(e.getMessage());
//			e.printStackTrace();
//		} catch (SQLException e) {
//			log.warn(e.getMessage());
//			e.printStackTrace();
//		} finally {
//			try {
//				if(conn != null)
//					conn.close();
//			} catch(SQLException e) {
//				log.warn(e.getMessage());
//				e.printStackTrace();
//			} 
//		}
//	}
	
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
	
}
