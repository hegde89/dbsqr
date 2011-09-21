package edu.unika.aifb.dbsqr;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;

import edu.unika.aifb.dbsqr.index.DBIndexService;
import edu.unika.aifb.dbsqr.util.Timing;

public class Preprocessor {
	
	private static final Logger log = Logger.getLogger(Preprocessor.class);
	
	public static void main(String[] args) throws Exception {
		Calendar now = Calendar.getInstance();
		log.info("==================================================================================");
		SimpleDateFormat formatter = new SimpleDateFormat("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
		log.info("  It is now : " + formatter.format(now.getTime()));
		long start = System.currentTimeMillis();

		Config.setConfigFilePath("./res/config/config.cfg");
		Config config = Config.getConfig();
		Timing timing = new Timing(config.getTemporaryDirectory() + config.getDbName() + "/log");
		timing.init(156);

		DBIndexService indexBuilder = new DBIndexService(config, timing);
//		indexBuilder.createTripleTable();
//		indexBuilder.createDatasourceTable();
//		indexBuilder.createSchemaTable();
//		indexBuilder.createEntityTable();
//		indexBuilder.createEntityRelationTable();
//		int maxDistance = config.getMaxDistance();
//		for(int i = 2; i <= 2; i++) {
//			indexBuilder.createEntityRelationTable(i);
//		}
//		indexBuilder.createKeywordEntityLuceneIndex();
//		indexBuilder.createCompleteKeywordTable();
//		indexBuilder.createKeywordConceptConnectionTable();
//		indexBuilder.createKeywordEntityConnectionTable();
		indexBuilder.createKeywordSourceInclusionTable();
//		indexBuilder.createKeywordSourceConnectionTable();
		indexBuilder.close();

		long end = System.currentTimeMillis();
		log.info("Total Time customing: " + (double) (end - start) / (double) 1000 + "(sec)");
		log.info("Total Time customing: " + (double) (end - start) / (double) 60000 + "(min)");
		log.info("Total Time customing: " + (double) (end - start) / (double) 3600000 + "(h)\n");
	}	
}
