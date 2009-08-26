package edu.unika.aifb.dbsqr;

import org.apache.log4j.Logger;

import edu.unika.aifb.dbsqr.index.DbConfig;
import edu.unika.aifb.dbsqr.index.DsSelectIndexBuilder;
import edu.unika.aifb.dbsqr.index.KeywSearchIndexBuilder;
import edu.unika.aifb.dbsqr.index.KeywSearchIndexBuilder_2;

public class Preprocessor {
	
	private static final Logger log = Logger.getLogger(Preprocessor.class);
	
	public static void main(String[] args) throws Exception {

//		if (args.length != 1) {
//			System.out.println("java Preprocess configFilePath(String)");
//			return;
//		}
		long start = System.currentTimeMillis();

		DbConfig.setConfigFilePath("./res/config/config.cfg");
		DbConfig config = DbConfig.getConfig();

		DsSelectIndexBuilder indexBuilder = new DsSelectIndexBuilder(config);
		indexBuilder.createDatasourceTable();
		indexBuilder.createTripleTable();
		indexBuilder.createSchemaTable();
		indexBuilder.createEntityTable();
		indexBuilder.createEntityConceptMembershipTable();
		indexBuilder.createEntityRelationTable();
		int maxDistance = config.getMaxDistance();
		for(int i = 2; i <= maxDistance; i++) {
			indexBuilder.createEntityRelationTable(i);
		}
		indexBuilder.createKeywordEntityInclusionTable();

		long end = System.currentTimeMillis();
		System.out.println("Time customing: " + (double)(end - start)/(double)60000 + "(min)");
	}	
}
