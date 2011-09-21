package Experiments;

import edu.unika.aifb.dbsqr.Environment;

public class PrintSql {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		String sql = " select distinct " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + ", " +
//		"ER." + Environment.ENTITY_RELATION_VID_COLUMN + ", " +
//		"E1." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " + 
//		"E2." + Environment.ENTITY_CONCEPT_ID_COLUMN + ", " +  
//		"E1." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//		"E2." + Environment.ENTITY_DS_ID_COLUMN + ", " + 
//		"(" + "2*" + 1.0 + ")/(" + 1 + "+1), " + 
//		Environment.TERM_PAIR_COMPOUND_COMPOUND + // compound keyword and compound keyword -- entity id and entity id 
//		" from " + Environment.ENTITY_RELATION_TABLE + "_1_4" + " as ER, " +	 	
//			Environment.ENTITY_TABLE + " as E1, " + 
//			Environment.ENTITY_TABLE + " as E2 " +
//		" where " + "ER." + Environment.ENTITY_RELATION_UID_COLUMN + " = " + 
//			"E1." + Environment.ENTITY_ID_COLUMN + 
//		" and " + "ER." + Environment.ENTITY_RELATION_VID_COLUMN + " = " + 
//			"E2." + Environment.ENTITY_ID_COLUMN; 
		
//		String sql = "alter table " + Environment.KEYWORD_CONCEPT_CONNECTION_TABLE + 
//			" add index " + 
////			Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_UID_COLUMN + ", " +
//			Environment.KEYWORD_CONCEPT_CONNECTION_KEYWORD_VID_COLUMN + ", " +
//			Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_UID_COLUMN + ", " + 
//			Environment.KEYWORD_CONCEPT_CONNECTION_CONCEPT_VID_COLUMN + ", " + 
//			Environment.KEYWORD_CONCEPT_CONNECTION_TYPE_COLUMN;
//		System.out.print(sql);
		
//		String sql = "select distinct " + Environment.TRIPLE_SUBJECT_COLUMN + ", " + 
//			Environment.SCHEMA_ID_COLUMN + ", " + Environment.TRIPLE_DS_ID_COLUMN + 
//			" from " + Environment.TRIPLE_TABLE + ", " + Environment.SCHEMA_TABLE +  
//			" where (" + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
//			" or " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.DATA_PROPERTY + ") " + 
//			" and " + Environment.SCHEMA_TYPE_COLUMN + " = " + Environment.TOP_CLASS + 
//			" and " + Environment.SCHEMA_DS_ID_COLUMN + " = " + Environment.TRIPLE_DS_ID_COLUMN;
//		System.out.print(sql);
		
		String updateSql = "update " + Environment.TRIPLE_TABLE + " as A, " + 
			Environment.DATASOURCE_TABLE + " as B " +
			" set " + "A." + Environment.TRIPLE_DS_ID_COLUMN + " = " + "B." + 
			Environment.DATASOURCE_ID_COLUMN + 
			" where " + "A." + Environment.TRIPLE_DS_COLUMN + " = " + "B." + 
			Environment.DATASOURCE_NAME_COLUMN + 
		" and " + "A." + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.UNPROCESSED;
		System.out.print(updateSql);
		
		
//		5	cyc.com	352738
//		6	deri.org	321096
//		7	codehaus.org	263998
//		8	qdos.com	202443
//		9	twit.tv	162266
//		10	kanzaki.com	149665
//		11	dbtune.org	148584
//		12	ajft.org	113091
//		13	opera.com	103861
//		int i = (352738 + 321096 + 263998 + 202443 + 162266 + 149665 + 148584 + 103861)/8; 
//		System.out.print(i);

	}

}
