/*
 * Reducer class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created by: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/22 21:59
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text>
{
    static List<String> postingList = new ArrayList<String>();
    static Text CurrentItem = new Text(" ");

    static Configuration conf = new Configuration();

    static List<Put> putList = new ArrayList<Put>();

    public static class HBase{
        private static Configuration conf = null;
        static
        {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", "2181");
        }

	public static void addData (String tableName, String rowKey,
				String family, String qualifier, String value) throws IOException{
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert record success!");
		}
		
		public void addDatas (String tableName, List<Put> putList) throws IOException{
			HTable table = new HTable(conf, tableName);
			table.put(putList);
			System.out.println("insert record success!");
		}

    }

    public void addData(String tableName, String rowKey, String family, String qualifier, String value) throws IOException
    {
        try{
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
	int sum = 0;
	Text word1 = new Text();
	Text word2 = new Text();
	word1.set(key.toString().split("#")[0]);//word
	String temp = new String();
	temp = key.toString().split("#")[1];//filename

	for(IntWritable val : values)
	    sum += val.get();

	word2.set(temp + ":" + sum);


	if(!CurrentItem.equals(word1) && (!CurrentItem.equals(" ")))
	{
	    StringBuffer out = new StringBuffer();
	    long cnt_word = 0;
	    long cnt_file = 0;
	    for(String p:postingList)
	    {
		out.append(p);
		out.append("; ");
		long num =  Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
		if(num >0)
		    cnt_file++;
		cnt_word += num;
	    }

	    if(cnt_word > 0)
		    context.write(CurrentItem, new Text(out.toString()));

	    if(cnt_file > 0)
        {
	        double fre = (double)(cnt_word)/(double)cnt_file;
            //addData("Wuxia", word1.toString(), "content", "frequency", Double.toString(fre));
            Put put = new Put(Bytes.toBytes(word1.toString()));
            put.add(Bytes.toBytes(new String("content")),Bytes.toBytes(new String("frequency")),Bytes.toBytes(Double.toString(fre)));
            putList.add(put);
        }

	    postingList = new ArrayList<String>();
	}
	CurrentItem = new Text(word1);
	postingList.add(word2.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	StringBuffer out = new StringBuffer();

	long cnt_word = 0;
	long cnt_file = 0;
	for(String p:postingList)
	{
	    out.append(p);
	    out.append(" ; ");
	    long num =  Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
	    if(num >0)
		cnt_file++;
	    cnt_word += num;
	}

	if(cnt_word > 0)
	    context.write(CurrentItem, new Text(out.toString()));

	if(cnt_file > 0)
    {
	        double fre = (double)(cnt_word)/(double)cnt_file;
            Put put = new Put(Bytes.toBytes(CurrentItem.toString()));
            put.add(Bytes.toBytes(new String("content")),Bytes.toBytes(new String("frequency")),Bytes.toBytes(Double.toString(fre)));
            putList.add(put);
    }
    HBase hbase = new HBase();
    try{
        hbase.addDatas("Wuxia",putList);
    }
    catch (IOException e)
    {
        e.printStackTrace();
    }
}
}
