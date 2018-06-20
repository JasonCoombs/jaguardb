import com.jaguar.jdbc.JaguarDataSource;
import java.io.*;
import java.sql.DatabaseMetaData;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import com.jaguar.jdbc.JaguarResultSetMetaData;
import java.sql.ResultSetMetaData;
import com.jaguar.jdbc.JaguarDriver;
import java.sql.DriverManager;
import java.util.Properties;


// Make sure libJaguarClient.dll is in current folder
// or copied to C:\Windows\System32\ folder
// The argument to this program is the server and port of server
// Insert, update, delete commands should call executeUpdate()
// Select commands should call executeQuery()

public class JaguarJDBCTest 
{
	public static void main( String [] args ) {
           if ( args.length < 2 ) {
               System.out.println("Please enter " + args[0] + " [IP] [Port]");
               System.exit(1);
           }
		System.loadLibrary("JaguarClient");
		JaguarJDBCTest t = new JaguarJDBCTest();
		try {
			          System.out.println( "t.testJaguarDataSource ...\n");
			t.testJaguarDataSource( args[0], args[1] );
		} catch ( SQLException e ) {
			System.out.println( "Exception:\n");
			e.printStackTrace();
		}


		try {
			System.out.println( "\n");
			System.out.println( "t.testJaguarDataSourcePrepare ....................\n");
			t.testJaguarDataSourcePrepare( args[0], args[1] );
		} catch ( SQLException e ) {
		}
		
		try {
			System.out.println( "\n");
			System.out.println( "t.testMetaData ...................................\n");
			t.testMetaData( args[0], args[1] );
		} catch ( SQLException e ) {
		}

		try {
			System.out.println( "\n");
			System.out.println( "t.testDriver .........................................\n");
			t.testDriver( args[0],  args[1] );
		} catch ( SQLException e ) {
		}

		try {
			System.out.println( "\n");
			System.out.println( "t.testDriverManager .........................................\n");
			t.testDriverManager( args[0],  args[1] );
		} catch ( SQLException e ) {
		}
	}


    public void testJaguarDataSource( String IP, String ports ) throws SQLException 
	{
        DataSource ds = new JaguarDataSource( IP, Integer.parseInt( ports ), "test");  
		ds.setLoginTimeout( 10 ); // seconds
        Connection connection = ds.getConnection("admin", "jaguar");  // username and password
		Statement statement = connection.createStatement();
		String key, val;
		String m1;
		String addr;

		ResultSet rs;
		statement.executeUpdate("insert into jbench values ( k1k1, v1v1 ) ; " );
		rs = statement.executeQuery("select * from jbench limit 10;");
		while(rs.next()) {
			key = rs.getString("uid");
			val = rs.getString("addr");
			System.out.println( "uid: " + key + " addr: " + val  );
		}

		statement.executeUpdate("update jbench set addr=123456 where uid='k1k1'");
		rs = statement.executeQuery("select * from jbench limit 10;");
		while(rs.next()) {
			key = rs.getString("uid");
			addr = rs.getString("addr");
			System.out.println( "uid: " + key + " addr: " + addr  );
		}


		System.out.println( "test ResultSetMetaData before next ...");
		ResultSetMetaData resmeta1 = rs.getMetaData();
		int ct = resmeta1.getColumnType( 1 );
		String n1 = resmeta1.getColumnName(1);
		System.out.println( "colcount =" + ct );
		System.out.println( "colname n1=" + n1 );

		rs = statement.executeQuery("select * from jbench limit 10;");
		while(rs.next()) {
			key = rs.getString("uid");
			m1 = rs.getString("k2");
			addr = rs.getString("addr");
			System.out.println( "uid: " + key + " m1: " + m1 + " addr: " + addr  );
		}

		System.out.println( "test ResultSetMetaData after next ...");
		ResultSetMetaData resmeta = rs.getMetaData();
		ct = resmeta.getColumnType( 1 );
		n1 = resmeta.getColumnName(1);
		System.out.println( "colcount =" + ct );
		System.out.println( "colname n1=" + n1 );

		rs.close();
		statement.close();
		connection.close();
    }

    public void testJaguarDataSourcePrepare( String IP, String ports ) throws SQLException 
	{
        DataSource ds = new JaguarDataSource(IP, Integer.parseInt( ports ), "test");  
		ds.setLoginTimeout( 13 ); // seconds
        Connection connection = ds.getConnection("admin", "jaguar");  // username and password
		ResultSet rs = connection.prepareStatement("select * from jbench limit 10").executeQuery();

		String val;
		String m1;
		while(rs.next()) {
			val = rs.getString("uid");
			m1 = rs.getString("m1");
			System.out.println( "uid: " + val + " m1: " + m1  );
		}

		System.out.println( "test ResultSetMetaData after next ...");
		ResultSetMetaData resmeta = rs.getMetaData();
		int ct = resmeta.getColumnType( 1 );
		String n1 = resmeta.getColumnName(1);
		System.out.println( "coltype=" + ct );
		System.out.println( "colname=" + n1 );

		ct = resmeta.getColumnType( 2 );
		n1 = resmeta.getColumnName(2);
		System.out.println( "coltype=" + ct );
		System.out.println( "colname=" + n1 );

		rs.close();
		connection.close();
    }


    public void testMetaData( String IP, String ports ) throws SQLException 
	{
		DatabaseMetaData metadata = null;
        DataSource ds = new JaguarDataSource(IP, Integer.parseInt( ports ), "test");  
		ds.setLoginTimeout( 14 ); // seconds
        Connection connection = ds.getConnection("admin", "jaguar");  
		Statement statement = connection.createStatement();
		
		metadata = connection.getMetaData();

 		System.out.println("Database Product Name: " + metadata.getDatabaseProductName());
        System.out.println("Database Product Version: " + metadata.getDatabaseProductVersion());
        System.out.println("Logged User: " + metadata.getUserName());
        System.out.println("JDBC Driver: " + metadata.getDriverName());
        System.out.println("Driver Version: " + metadata.getDriverVersion());
        System.out.println("\n");

		String   catalog          = null;
		String   schemaPattern    = null;
		String   tableNamePattern = null;
		String[] types            = null;

		System.out.println("metadata.getTables ... ");
		ResultSet result = metadata.getTables( catalog, schemaPattern, tableNamePattern, types );
		while(result.next()) {
    		String tablecat = result.getString("TABLE_CAT");
    		String tableName = result.getString("TABLE_NAME");
    		String tableType = result.getString("TABLE_TYPE");
        	System.out.println("table: " + tableName + " cat: " + tablecat + " type: " + tableType );
		}
		System.out.println("metadata.getTables done ");
		result.close();

		System.out.println("metadata.getColumns ...");
		result = metadata.getColumns( catalog, schemaPattern, "jbench", "" );
		while(result.next()) {
    		String tableName = result.getString("TABLE_NAME");
    		String dtype = result.getString("DATA_TYPE");
    		String colname = result.getString("COLUMN_NAME");
    		String typename = result.getString("TYPE_NAME");
    		String sz = result.getString("COLUMN_SIZE");
    		String dec = result.getString("DECIMAL_DIGITS");

        	System.out.println("getcolumns: table: " + tableName + " col: " + colname );
        	System.out.println("    datatype: " + dtype + " typename: " + typename );
        	System.out.println("    size: " + sz + " decimaldigits: " + dec );
		}
		System.out.println("metadata.getColumns done ");
		result.close();

		System.out.println("metadata.getCatalogs ...");
		result = metadata.getCatalogs ();
		while(result.next()) {
    		String dbName = result.getString("TABLE_CAT");
        	System.out.println("db: " + dbName );
		}
		System.out.println("metadata.getCatalogs done ");
		result.close();

		statement.close();
		connection.close();
    }


    public void testDriverManager( String IP, String ports ) throws SQLException 
	{
	    try {
         	Class.forName("com.jaguar.jdbc.JaguarDriver");
        } catch ( ClassNotFoundException e ) {
            System.out.println( e.getMessage() );
        }
	
		String url = "jdbc:jaguar://" + IP + ":" + ports + "/test";
		Connection connection = DriverManager.getConnection( url, "admin", "jaguar" );
		Statement stmt = connection.createStatement();

		System.out.println("drop table t12 ... ");
        try { stmt.executeUpdate("drop table if exists t12"); } catch (Exception e) 
		{ System.out.println( e.getMessage() ); }

        stmt.executeUpdate("create table if not exists t12 ( key: id char(10), value: test char(20));");
		System.out.println("Done create table ");

        stmt.executeUpdate("insert into t12 (id, test) values ( '111', 'fdfdfd1' ); ");
        stmt.executeUpdate("insert into t12 (id, test) values ( '112', 'ccccc' ); ");
		System.out.println("Done, insert. executeQuery ...");

        ResultSet rs = stmt.executeQuery("select * from t12;");
		String k;
		String v;
        while ( rs.next() ) {
			k = rs.getString("id");
			v = rs.getString("test");
			System.out.println( "key: "+k + " value: " + v );
        }
		rs.close();

		System.out.println("desc t12 ...");
        rs = stmt.executeQuery("desc t12;");
		String line;
        while ( rs.next() ) {
			line = rs.getString("");
			// line = rs.getString(null);
			System.out.println( line );
        }
		rs.close();
		stmt.close();

		connection.close();
    }

    public void testDriver( String IP, String ports ) throws SQLException 
	{
	    try {
         	Class.forName("com.jaguar.jdbc.JaguarDriver");
        } catch ( ClassNotFoundException e ) {
            System.out.println( e.getMessage() );
        }
	
		String url = "jdbc:jaguar://" + IP + ":" + ports + "/test";
		// DriverManager jagD = new JaguarDriver();
		JaguarDriver jagD = new JaguarDriver();
		// jagD.setLoginTimeout( 21 );

		// Connection connection = DriverManager.getConnection( url, "admin", "jaguar" );
		Properties prop = new Properties();
		prop.put("user", "admin" );
		prop.put("password", "jaguar" );
		prop.put("timeout", "34" );
		Connection connection = jagD.connect( url, prop );
		Statement stmt = connection.createStatement();

		System.out.println("drop table t12 ... ");
        try { stmt.executeUpdate("drop table if exists t12"); } catch (Exception e) 
		{ System.out.println( e.getMessage() ); }

        stmt.executeUpdate("create table if not exists t12 ( key: id char(10), value: test char(20));");
		System.out.println("Done create table ");

        stmt.executeUpdate("insert into t12 (id, test) values ( '111', 'fdfdfd1' ); ");
        stmt.executeUpdate("insert into t12 (id, test) values ( '112', 'ccccc' ); ");
		System.out.println("Done, insert. executeQuery ...");

        ResultSet rs = stmt.executeQuery("select * from t12;");
		String k;
		String v;
        while ( rs.next() ) {
			k = rs.getString("id");
			v = rs.getString("test");
			System.out.println( "key: "+k + " value: " + v );
        }
		rs.close();

		stmt.close();
		connection.close();
    }

}
