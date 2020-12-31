import java.io.IOException;
import java.util.*;
import javax.sql.*;
import java.sql.*;
import com.jaguar.jdbc.*;
import java.util.concurrent.TimeUnit;

public class TestJaguar {
    private static final String IP = "192.168.7.120";
	private static final int  PORT = 5555;
	private static final String DBNAME = "test";
	private static final String USER = "admin";
	private static final String PASSWORD = "jaguar";
    private static final int COUNTINSERT = 10000000; //how many data rows are inserted
    private static final int COUNTSELECT = 100000; // how many times point query is conducted
    public static void main (String[] args)
    {
        try {
            DataSource ds = new JaguarDataSource( IP, PORT, DBNAME);
            Connection connection = ds.getConnection( USER, PASSWORD);
            Statement statement = connection.createStatement();
			statement.executeUpdate("drop table test;");
			TimeUnit.SECONDS.sleep(2);
			System.out.println("Creating table test in Jaguar Database ...");
            statement.executeUpdate("create table test ( key : uid char(32), value : addr char(68));");
			TimeUnit.SECONDS.sleep(2);
            long timeInsert = testInsert (connection, statement);
            connection = ds.getConnection( USER, PASSWORD);
            statement = connection.createStatement();
            long timeSelect = testSelect (connection, statement);
			System.out.println("\nSummary:");
			System.out.println("Insert " + COUNTINSERT + " data rows to Jaguar Database : " + timeInsert + "s." );
			System.out.println("Select " + COUNTSELECT + " rows from Jaguar Database: " + timeSelect + "s." );
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    /*
     Test Point Querying one row with a specified key from Jaguar Database
     
     @return  how many seconds used to conduct point query for COUNTSELECT times
     */
    
    private static long testSelect (Connection connection, Statement statement) throws Exception 
	{
        String oneKey = "r1";	
		Random random = new Random();
        String value;
		System.out.println("Begin selecting data ...");
		TimeUnit.SECONDS.sleep(2);
        long startTime = System.currentTimeMillis();
        for ( int i=1; i <= COUNTSELECT ; i++) {
		    oneKey = "r" + String.format("%031d",random.nextInt(COUNTINSERT)+1);
            ResultSet rs = statement.executeQuery("select * from test where uid='" + oneKey +"';");
			while ( rs.next() ) {
                value = rs.getString("addr");
                System.out.println("uid : " + oneKey + " , addr : " + value);
            }
        }
        long endTime = System.currentTimeMillis();
		System.out.println("Select " + COUNTSELECT + " rows from Jaguar Database: " + (endTime-startTime)/1000 + "s." );
		return (endTime-startTime)/1000;
    }
    
    /*
     Test inserting COUNTINSERT rows of data to Jaguar Database.
     
     @return  how many seconds used to insert data
     */
    
    private static long testInsert (Connection connection, Statement statement) throws Exception 
	{
        String key = "r" + Integer.toString(1);
        String value;
        System.out.println("Begin inserting " + COUNTINSERT + " rows of data to Jaguar ...");
        long startTime = System.currentTimeMillis();
        for ( int i=1; i <= COUNTINSERT; i++ ) {
            key = "r" + String.format("%031d",i);
            value = "12345678901234567890123456789012345678901234567890123456789012345678";
            statement.executeUpdate("insert into test (uid, addr) values ('" + key + "' , '" + value + "');" );
        }
        statement.close();
        connection.close();
        long endTime = System.currentTimeMillis();
        System.out.println("Insert " + COUNTINSERT + " data rows to Jaguar Database : " + (endTime-startTime)/1000 + "s." );	
		return (endTime-startTime)/1000;
    }
}
