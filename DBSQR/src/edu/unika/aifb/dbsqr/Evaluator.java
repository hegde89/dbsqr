package edu.unika.aifb.dbsqr;

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;

import edu.unika.aifb.dbsqr.search.DBSearchServiceByConceptConnection;
import edu.unika.aifb.dbsqr.search.DBSearchServiceByEntityConnection;
import edu.unika.aifb.dbsqr.search.DBSearchServiceByKeywordSet;
import edu.unika.aifb.dbsqr.search.DBSearchServiceBySourceConnection;
import edu.unika.aifb.dbsqr.search.KeywordRoutingPlan;
import edu.unika.aifb.dbsqr.search.KeywordRoutingPlan.Keyword2DatasourceId;
import edu.unika.aifb.dbsqr.util.Timing;

public class Evaluator {
	
	private final static Logger log = Logger.getLogger(Evaluator.class);
	
	public static void main(String[] args) {

		Config.setConfigFilePath("./res/config/config.cfg");
		Config config = Config.getConfig();
		Timing timing = new Timing(config.getTemporaryDirectory() + config.getDbName() + "/eva_log");

		DBSearchServiceByKeywordSet searcher = new DBSearchServiceByKeywordSet(config);
		Scanner scanner = new Scanner(System.in);
		while(true) {
			System.out.println("Please input the keywords:");
			String line = scanner.nextLine();
			if(line.startsWith("exit"))
				break;
			String tokens [] = line.split(" ");
			LinkedList<String> keywordList = new LinkedList<String>();
			for(int i=0;i<tokens.length;i++) {
				keywordList.add(tokens[i]);
			}
			
			long start, end;
			
//			for(int i = 0; i < 20; i++) {
//				searcher.retrievingEntitySetConnections(keywordList, 10);
//			}
//			start =  System.currentTimeMillis();
//			for(int i = 0; i < 20; i++) {
//				searcher.retrievingEntitySetConnections(keywordList, 10);
//			}
//			end = System.currentTimeMillis();
//			log.info("Time for Retrieving Entity Set Connections: " + (end - start)/20 + "(ms)");
//			log.info("Time for Retrieving Entity Set Connections: " + (double) (end - start)/20000  + "(sec)");
			
//			for(int i = 0; i < 20; i++) {
//				searcher.computeKeywordRoutingPlans(keywordList, 10);
//			}
//			start =  System.currentTimeMillis();
//			for(int i = 0; i < 20; i++) {
//				searcher.computeKeywordRoutingPlans(keywordList, 10);
//			}
//			end = System.currentTimeMillis();
			
			start =  System.currentTimeMillis();
			List<KeywordRoutingPlan> plans = searcher.computeKeywordRoutingPlans(keywordList, 10);
			end = System.currentTimeMillis();			
			
			log.info("Time for Getting the Top-k Keyword Routing Plans: " + (end - start) + "(ms)");
			log.info("Time for Getting the Top-k Keyword Routing Plans: " + (double) (end - start)/1000  + "(sec)");
			System.out.println(plans.size());
			for(KeywordRoutingPlan plan : plans) {
				for(Keyword2DatasourceId keyw2ds : plan.getKeywordRoutingPlan()) {
					System.out.println(keyw2ds.getKeyword() + "\t" + keyw2ds.getDsId());
				}
				System.out.println();
			}
		}
		searcher.close();
		
	}

}
