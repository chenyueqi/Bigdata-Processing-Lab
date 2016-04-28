/*
 * Map class for TFIDF
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/28 18:58
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

public class TFIDFMapper extends Mapper<Object, Text, Text, IntWritable>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
	/*get split from RecordReader*/
	FileSplit fileSplit = (FileSplit) context.getInputSplit();

	Pattern p = Pattern.compile("(((\\.txt)|(\\.TXT))(\\.segmented))|(\\.)");
	Matcher m = p.matcher(fileSplit.getPath().getName());
	String fileName = m.replaceAll("");

	Pattern p2 = Pattern.compile("\\d+");
	String[] authorfile = p2.split(fileName);


	String temp = new String();
	String line = value.toString();
	StringTokenizer itr = new StringTokenizer(line);
	for(; itr.hasMoreTokens();)
	{
	    Text word = new Text();
	    word.set(itr.nextToken() + "#" + authorfile[0] + "#" + authorfile[1]);
	    context.write(word, new IntWritable(1));

	    /*OutputFormat: <word#author#filename, 1> */
	}
    }

}
