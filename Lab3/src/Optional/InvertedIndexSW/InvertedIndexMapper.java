/*
 * Map class for InvertedIndex Table
 * Project Name: Inverted Index Table with stop word table from HBase for Hadoop (Lab3)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/5/12 17:17
*/


import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable>
{
    private Set<String> stopwords;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.quo.rum", "localhost");
	conf.set("hbase.zookeeper.property.clientPort", "2181");

	stopwords = new TreeSet<String>();
/*
	HTable table = new HTable(conf, "StopWords");
	
	Scan s = new Scan();
	ResultScanner ss = table.getScanner(s);

	for (Result r : ss)
	    for(KeyValue kv : r.raw())
		stopwords.add(new String(kv.getValue()));
		*/
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
	/*get split from RecordReader*/
	FileSplit fileSplit = (FileSplit) context.getInputSplit();

	Pattern p = Pattern.compile("(((\\.txt)|(\\.TXT))(\\.segmented))|(\\.)");
	Matcher m = p.matcher(fileSplit.getPath().getName());
	String fileName = m.replaceAll("");

	String line = value.toString();
	StringTokenizer itr = new StringTokenizer(line);
	for(; itr.hasMoreTokens();)
	{
	    String temp = itr.nextToken();
	//    if(!stopwords.contains(temp))
	    {
		Text word = new Text();
		word.set(temp + "#" + fileName);
		context.write(word, new IntWritable(1));
	    }

	    /*OutputFormat: <word#filename, 1> */
	}
    }

}
