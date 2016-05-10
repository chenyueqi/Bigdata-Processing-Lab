/*************************************************************************
	> File Name: ReadHbase.java
	> Author:  Wei Liu 
	> Mail: liuwei13cs@smail.nju.edu.cn
	> Created Time: 2016年05月10日 星期二 11时32分45秒
 ************************************************************************/
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class ReadHbase {
	private static Configuration conf = null;
	static
	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quo.rum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
	public static void scanRows(String tableName) throws IOException
    {
		File writename = new File("output"); 
        writename.createNewFile();  
        BufferedWriter out = new BufferedWriter(new FileWriter(writename));
		HTable table = new HTable(conf,tableName);
		Scan s = new Scan();
		ResultScanner ss = table.getScanner(s);
		String column = " ";

		
		for(Result r : ss)
        	{
			for(KeyValue kv : r.raw())
            {
                out.write(new String(kv.getRow()));
                out.write('\t');
                out.write(new String(kv.getValue()));
                out.write('\n');
            }

			out.flush();
		}
		out.close();
	}
	
	public static void main (String [] args) throws IOException
    {
		ReadHbase.scanRows("Wuxia");
	}
}

