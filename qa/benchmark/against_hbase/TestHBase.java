import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class TestHBase 
{
    private static final int COUNTPUT = 10000000; // how many times to do PUT
	private static final int COUNTGET = 3000000; // how many times to do GET
    public static void main (String[] args) 
	{
		try {    
	        Configuration config = HBaseConfiguration.create();
		    Connection connection = ConnectionFactory.createConnection(config);
		
            String tableName = "test";
		    
            Admin admin = connection.getAdmin();
            if ( admin.isTableAvailable(TableName.valueOf(tableName)) ) {
			    System.out.println("HBase Table " + tableName + " exists.");
				System.out.println("Disabling Table " + tableName + " ...");
                admin.disableTable(TableName.valueOf(tableName));
                TimeUnit.SECONDS.sleep(5);
				System.out.println("Dropping  Table " + tableName + " ...");
                admin.deleteTable(TableName.valueOf(tableName));
                TimeUnit.SECONDS.sleep(5);
            }
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            desc.addFamily(new HColumnDescriptor("c1"));
            admin.createTable(desc);
			System.out.println("Table " + tableName + " created successfully.");
			Table table = connection.getTable(TableName.valueOf(tableName));
            
            //long timePut = testPut(table, COUNTPUT);
            long timeBatchPut = testBatchPut(table, COUNTPUT);
			//long timeGet = testGet(table, COUNTGET);
			System.out.println("\nSummary: ");
            //System.out.println("Insert " + COUNTPUT + " data rows to HBase : " + timePut + "s." );
			System.out.println("Batch Insert " + COUNTPUT + " data rows to HBase : " + timeBatchPut + "s." );
            //System.out.println("Get " + COUNTGET+ " data rows from HBase: "  + timeGet + "s." );
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    /*
     Test the performence of Put operation of HBase.
     @param table  the Table to put data to
     @param count  how many rows to put into HBase
     @return       how many seconds to cost
     */
	private static long testPut( Table table, int count ) throws Exception 
	{
        Put put = new Put(Bytes.toBytes("r1"));
		String value = "1";
        long startTime = System.currentTimeMillis();
        System.out.println("Begin putting " + count + " data rows to HBase ...");
        for (int i=1; i<=count; i++) {
            put = new Put(Bytes.toBytes("r" + String.format("%031d",i)));
			value = "12345678901234567890123456789012345678901234567890123456789012345678";
            put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("q1"), Bytes.toBytes(value));
            table.put(put);
        }
        long endTime = System.currentTimeMillis();
        
        System.out.print("Insert " + count + " data entries to HBase : " );
        System.out.print( (endTime-startTime)/1000 );
        System.out.println("s");
        return (endTime-startTime)/1000;
	}

    /*
	 Test the performence of Batch Put operation of HBase.
     @param table  the Table to put data to
	 @param count  how many rows to put into HBase
     @return       how many seconds to cost
	 */
    private static long testBatchPut(Table table, int count) throws Exception
	{
		Put put = new Put(Bytes.toBytes("r1"));
		String value = "";
		List<Row> batch = new ArrayList<Row>();
		long startTime = System.currentTimeMillis();
		System.out.println("Begin Batch putting " + count + " data rows to HBase ...");
		Object[] tempResult = new Object[10000];
		for (int i=1; i<=count; i++) {
		    put = new Put(Bytes.toBytes("r" + String.format("%031d",i)));
			value = "12345678901234567890123456789012345678901234567890123456789012345678";
	        put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("q1"), Bytes.toBytes(value));
			batch.add(put);
			if (i%10000 == 0) {
				table.batch(batch, tempResult);
				batch.clear();
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.print("Batch Insert " + count + " data entries to HBase : " );
		System.out.print( (endTime-startTime)/1000 );
		System.out.println("s");
		return (endTime-startTime)/1000;
	}

    /*
     Test the performence of Get operation of HBase.
     @param table  the Table to conduct Get operation on
     @param count  how many times to conduct Get operation
     @return       how many seconds to cost
     */
	private static long testGet(Table table, int count) throws Exception 
	{
        String oneKey = "r";
		Random random = new Random();
		Get get = new Get( Bytes.toBytes( oneKey ) );
		long startTime = System.currentTimeMillis();
        System.out.println("Begin getting " + oneKey + " for " + count + " times ...");
		for (int i=1; i<count; i++) {
		    oneKey="r" + String.format("%031d", random.nextInt(COUNTPUT)+1);
			get = new Get( Bytes.toBytes( oneKey ) );
			get.addColumn( Bytes.toBytes("c1"), Bytes.toBytes("q1") );
		    Result result = table.get(get);
			System.out.print(oneKey + ": ");
		    for( Map.Entry<byte[],byte[]> entry : result.getFamilyMap(Bytes.toBytes("c1")).entrySet()){
				System.out.println(Bytes.toString(entry.getKey()) + ":" + Bytes.toString(entry.getValue()));			    
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.print("Get " + COUNTGET+ " data rows from HBase: " );
		System.out.print( (endTime-startTime)/1000 );
		System.out.println("s");
        return (endTime-startTime)/1000;
	}
}
