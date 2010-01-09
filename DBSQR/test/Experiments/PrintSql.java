package Experiments;

import edu.unika.aifb.dbsqr.Environment;

public class PrintSql {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String selectSql = "select count(distinct " + Environment.TRIPLE_SUBJECT_ID_COLUMN + ", " + 
				Environment.TRIPLE_OBJECT_ID_COLUMN + ", " + 
				Environment.SCHEMA_FREQ_COLUMN + " > " + Environment.THRESHOLD_RELATION_FREQ_PRUNED + ")" +  
			" from " + Environment.TRIPLE_TABLE + ", " + Environment.SCHEMA_TABLE + 
			" where " + Environment.TRIPLE_PROPERTY_TYPE + " = " + Environment.OBJECT_PROPERTY + 
			" and " + Environment.TRIPLE_DS_ID_COLUMN + " = " + 2 + 
			" and " + Environment.TRIPLE_OBJECT_ID_COLUMN + " <> 0 " + 
			" and " + Environment.TRIPLE_SUBJECT_ID_COLUMN + " <> 0 " + 
			" and " + Environment.SCHEMA_TYPE_COLUMN + " = " + Environment.OBJECT_PROPERTY +
			" and " + Environment.TRIPLE_DS_ID_COLUMN + " = " + Environment.SCHEMA_DS_ID_COLUMN +
			" and " + Environment.TRIPLE_PROPERTY_COLUMN + " = " + Environment.SCHEMA_URI_COLUMN;
		System.out.print(selectSql);

	}

}
