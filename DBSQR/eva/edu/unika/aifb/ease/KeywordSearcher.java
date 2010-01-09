package edu.unika.aifb.ease;

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import edu.unika.aifb.ease.search.DBSearchService;

public class KeywordSearcher {

	public static void main(String[] args) {
		
//		if (args.length != 1) {
//			System.out.println("java KeywordSearcher configFilePath(String)");
//			return;
//		}

		Config.setConfigFilePath("./res/config/config.cfg");
		Config config = Config.getConfig();

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
			List<String> centers = searcher.getMaxRRadiusGraphCenters(keywordList, config.getTopResult());
//			for(String center : centers) {
//				System.out.println(center);
//			}
			System.out.println("Number of Graphs: " + centers.size());
		}
		searcher.close();
		
	}

}
