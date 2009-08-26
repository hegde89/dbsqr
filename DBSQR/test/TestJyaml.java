import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;

public class TestJyaml {
	
	private static final Logger log = Logger.getLogger(TestJyaml.class); 

	public static void main(String[] args) {
		try {
			Map config = (Map)Yaml.load(new File("./res/config/config.cfg"));
			String server = config.get("server") != null ? (String)config.get("server") : "localhost";
			String userName = config.get("username") != null ? (String)config.get("username") : "root";
			String password = config.get("password") != null ? (String)config.get("password") : "root";	
			int port = config.get("port") != null ? (Integer)config.get("port") : 3306;
			int maxDistance = config.get("maxDistance") != null ? (Integer)config.get("maxDistance") : 4;
			int topKeyword = config.get("topKeyword") != null ? (Integer)config.get("topKeyword") : 5;
			int topDatabase = config.get("topDatabase") != null ? (Integer)config.get("topDatabase") : 3;	
			Map<String,Set<String>> dsAndDataFiles = (Map<String,Set<String>>)config.get("datasources");
			
			log.debug("server: " + server);
			log.debug("userName: " + userName);
			log.debug("password: " + password);
			log.debug("port: " + port);
			log.debug("maxDistance: " + maxDistance);
			log.debug("topKeyword: " + topKeyword);
			log.debug("topDatabase: " + topDatabase);
			log.debug("datasources and data files: " + dsAndDataFiles);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	} 
}
