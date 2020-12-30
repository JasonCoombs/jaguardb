import com.jaguar.jdbc.internal.jaguar.Jaguar;
import java.io.*;
import java.util.concurrent.TimeUnit;

// usage: java example <port>
public class example
{
    public static void main(String[] args)
    {
		if ( args.length < 2 ) {
           	System.out.println( "Usage: java example server  port");
			System.exit(1);
		}

        System.out.println( "Server: " + args[0] );
        System.out.println( "Port: " + args[1] );

        System.loadLibrary("JaguarClient");  // need -Djava.library.path in Linux
        Jaguar client = new Jaguar();
        boolean rc = client.connect( args[0], Integer.parseInt( args[1] ), "admin", "jaguarjaguarjaguar", "test", "dummy", 0 );
		if ( ! rc ) {
           	System.out.println( "Connection error");
			System.exit(1);
		}
		
        rc = client.execute( "drop table if exists testjava");
        rc = client.execute( "create table testjava (key: uid char(16), value: addr char(32), city char(32) )  ");
        rc = client.execute( "insert into testjava values ( 'k1', 'v1', 'c1')" );
        rc = client.execute( "insert into testjava values ( 'k2', 'v2', 'c2' )" );
        rc = client.execute( "insert into testjava values ( 'k3, 'v3', 'c3' )" );
		try { TimeUnit.SECONDS.sleep(2); } catch ( Exception e) { System.out.println( "sleep error"); }
        rc = client.query( "select uid, addr, city from testjava limit 10;" );
        String val;
        while ( client.reply() ) {
            val = client.getValue(  "uid" );
            System.out.println( "uid=" + val );

            val = client.getValue(  "addr" );
            System.out.println( "addr=" + val );

            val = client.getNthValue( 1 );
            System.out.println( "1-th="+val );

            val = client.getValue( "v1" );
            System.out.println( "v1="+val );

            val = client.getNthValue( 2 );
            System.out.println( "2-nth="+val );

            val = client.getValue( "v2" );
            System.out.println( "v2="+val );

            val = client.getNthValue( 3 );
            System.out.println( "3-th="+val );

            val = client.getNthValue( 5 );
            System.out.println( "5-th=" + val );

            val = client.getNthValue( 0 );
            System.out.println( "0-th=" + val );

            val = client.getNthValue( -1 );
            System.out.println( "col -1=" + val );

            val = client.jsonString();
            System.out.println( "json=" + val );
        }

        if ( client.hasError( ) ) {
             String e = client.error( );
             System.out.println( e );
        }

        client.close();
    }
 }


