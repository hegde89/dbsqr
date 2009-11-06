import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.dbsqr.Environment;


public class TestCreateDB {

	private static final Logger log = Logger.getLogger(TestCreateDB.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/", "root", "root");
			stmt = conn.createStatement();
			
			DatabaseMetaData meta = conn.getMetaData();
			ResultSet rs = meta.getCatalogs();
			List<String> catalogs = new ArrayList<String>();
			while(rs.next()) {
				catalogs.add(rs.getString(1));
			}
			rs.close();
			log.debug(catalogs);
			
			log.info("---- Creating Database ----");
			if(catalogs.contains(Environment.DEFAULT_DATABASE_NAME)) {
				stmt.executeUpdate("drop database " + Environment.DEFAULT_DATABASE_NAME); 
			}
			String createDBSql = "create database " + Environment.DEFAULT_DATABASE_NAME;
			stmt.execute(createDBSql);
			stmt.close();
			
			conn.setCatalog(Environment.DEFAULT_DATABASE_NAME);
			stmt = conn.createStatement();
			stmt.execute("create table test(col int)");
		} catch(SQLException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if(stmt != null)
					stmt.close();
				if(conn != null)
					conn.close();
			} catch(SQLException e) {
				log.warn(e.getMessage());
				e.printStackTrace();
			} 
		}
	}

}
