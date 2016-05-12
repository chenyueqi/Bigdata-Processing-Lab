/*
 * class for write stopwords to HBase
 * Project Name: Inverted Index with stopword(Lab3)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/5/12 18:14
 */

import java.io.FileWriter;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class WriteHbase {
    private static Configuration conf = null;
    static
    {
	conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.quo.rum", "localhost");
	conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
	
    public static void WriteRows(String fileName, String tableName) throws IOException
    {   
	HTable table = new HTable(conf, tableName);

	File writefile = new File(fileName);
	InputStreamReader reader = new InputStreamReader(new FileInputStream(writefile));

	BufferedReader br = new BufferedReader(reader);
	String rowKey ;
	rowKey = br.readLine();

	while(rowKey != null)
	{
	    if(rowKey.length() != 0)
	    {
	    	try{
		    Put put = new Put(Bytes.toBytes(rowKey));
		    put.add(Bytes.toBytes("word"), Bytes.toBytes("content"), Bytes.toBytes("value"));

		    table.put(put);
		} catch(IOException e){
		    
		    e.printStackTrace();
		}
	    }
	    rowKey = br.readLine();
	}
    }
	
    public static void main (String [] args) throws IOException
    {
    	WriteHbase.WriteRows("Stop_words.txt", "StopWords");
    }
}

