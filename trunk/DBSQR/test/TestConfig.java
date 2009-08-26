import edu.unika.aifb.dbsqr.index.DbConfig;
import edu.unika.aifb.dbsqr.index.DbService;


public class TestConfig {
	public static void main(String[] args) {
		DbConfig.setConfigFilePath("./res/config/config.cfg");
		DbConfig config = DbConfig.getConfig();
		
		String server = config.getDbServer();
		String username = config.getDbUsername();
		String password = config.getDbPassword();
		String port = config.getDbPort();
		String dbName = config.getDbName();
		DbService service = new DbService(server, username, password, port, dbName, true);
		
	} 
}
