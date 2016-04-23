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

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text>
{
    static List<String> postingList = new ArrayList<String>();
    static Text CurrentItem = new Text(" ");

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

	word2.set("<" + temp + "," + sum + ">");


	if(!CurrentItem.equals(word1) && (!CurrentItem.equals(" ")))
	{
	    StringBuilder out = new StringBuilder();
	    long cnt = 0;
	    for(String p:postingList)
	    {
		out.append(p);
		out.append(";");
		cnt += Long.parseLong(p.substring(p.indexOf(",") +1, p.indexOf(">")));
	    }

	    /*TODO calculate average */
	    out.append("<total," + cnt + ">.");
	    if(cnt > 0)
		context.write(CurrentItem, new Text(out.toString()));
	    postingList = new ArrayList<String>();
	}
	CurrentItem = new Text(word1);
	postingList.add(word2.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	StringBuilder out = new StringBuilder();

	long cnt = 0;
	for(String p:postingList)
	{
	    out.append(p);
	    out.append(";");
	    cnt += Long.parseLong(p.substring(p.indexOf(",") +1, p.indexOf(">")));

	}
	out.append("<total," + cnt + ">.");
	if(cnt > 0)
	    context.write(CurrentItem, new Text(out.toString()));
    }
}
