package edu.unika.aifb.dbsqr;

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import edu.unika.aifb.dbsqr.search.DBSearchService;
import edu.unika.aifb.dbsqr.search.KeywordRoutingPlan;
import edu.unika.aifb.dbsqr.search.KeywordRoutingPlan.Keyword2DatasourceId;
import edu.unika.aifb.dbsqr.util.Timing;

public class Evaluator {
	
	public static void main(String[] args) {

		Config.setConfigFilePath("./res/config/config.cfg");
		Config config = Config.getConfig();
		Timing timing = new Timing(config.getTemporaryDirectory() + config.getDbName() + "/eva_log");

		DBSearchService searcher = new DBSearchService(config);
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
			List<KeywordRoutingPlan> plans = searcher.computeKeywordRoutingPlans(keywordList, 5);
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
