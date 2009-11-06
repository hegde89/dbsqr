import edu.unika.aifb.dbsqr.Config;
import edu.unika.aifb.dbsqr.db.DBService;


public class TestConfig {
	public static void main(String[] args) {
		Config.setConfigFilePath("./res/config/config.cfg");
		Config config = Config.getConfig();
		
		String server = config.getDbServer();
		String username = config.getDbUsername();
		String password = config.getDbPassword();
		String port = config.getDbPort();
		String dbName = config.getDbName();
		DBService service = new DBService(server, username, password, port, dbName, true);
		
	} 
}
