/*
 * Map class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/22 21:18
*/


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
	/*get split from RecordReader*/
	FileSplit fileSplit = (FileSplit) context.getInputSplit();
	String fileName = fileSplit.getPath().getName();
	String temp = new String();
	String line = value.toString();
	StringTokenizer itr = new StringTokenizer(line);
	for(; itr.hasMoreTokens();)
	{
	    Text word = new Text();
	    word.set(itr.nextToken() + "#" + fileName);
	    context.write(word, new IntWritable(1));

	    /*OutputFormat: <word#filename, 1> */
	}
    }

}
