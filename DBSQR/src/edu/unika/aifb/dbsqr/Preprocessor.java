package edu.unika.aifb.dbsqr;

import org.apache.log4j.Logger;

import edu.unika.aifb.dbsqr.index.DBIndexBuilder;

public class Preprocessor {
	
	private static final Logger log = Logger.getLogger(Preprocessor.class);
	
	public static void main(String[] args) throws Exception {

//		if (args.length != 1) {
//			System.out.println("java Preprocess configFilePath(String)");
//			return;
//		}
		long start = System.currentTimeMillis();

		Config.setConfigFilePath("./res/config/config.cfg");
		Config config = Config.getConfig();

		DBIndexBuilder indexBuilder = new DBIndexBuilder(config);
		indexBuilder.createTripleTable();
		indexBuilder.createDatasourceTable();
		indexBuilder.createSchemaTable();
		indexBuilder.createEntityTable();
		indexBuilder.createEntityRelationTable();
		indexBuilder.createKeywordEntityInclusionTableUsingLucene();
//		int maxDistance = config.getMaxDistance();
//		for(int i = 2; i <= maxDistance; i++) {
//			indexBuilder.createEntityRelationTable(i);
//		}
//		indexBuilder.createKeywordEntityInclusionTable();

		long end = System.currentTimeMillis();
		System.out.println("Time customing: " + (double)(end - start)/(double)1000 + "(sec)");
		System.out.println("Time customing: " + (double)(end - start)/(double)60000 + "(min)");
	}	
}
