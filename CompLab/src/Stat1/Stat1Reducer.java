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

public class Stat1Reducer extends Reducer<Text, IntWritable, Text, Text>
{
    static List<String> postingList = new ArrayList<String>();
    static Text CurrentItem = new Text(" ");

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
    	/*
    	if(key.toString().split("#")[0].equals("0"))
    	{
    	*/
    		int sum =0;
    		for (IntWritable val : values) {
    			sum += val.get();
    		}
    		Text result = new Text();
    		result.set(key.toString()+":"+sum);
    		context.write(result, new Text(""));
    		
    }
    	/*
    	if(key.toString().split("#")[0].equals("1"))
    	{
    		Text word = new Text();
    		word.set(key.toString().split("#")[1]);
    		int sum = 0;
    		Text word1 = new Text();
    		Text word2 = new Text();
    		word1.set(word.toString().split("@")[0]);
    		String temp = new String();
    		temp = key.toString().split("@")[1];

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
    				out.append("\t");
    				long num =  Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
    				if(num >0)
    					cnt_file++;
    				cnt_word += num;
    			}

    			if(cnt_file > 0)
    				out.insert(0, (double)(cnt_word)/(double)cnt_file + ", ");

    			if(cnt_word > 0)
    				context.write(CurrentItem, new Text(out.toString()));
    			postingList = new ArrayList<String>();
    		}
    		CurrentItem = new Text(word1);
    		postingList.add(word2.toString());
    	}
    }
    public void cleanup(Context context) throws IOException, InterruptedException
    {
	StringBuffer out = new StringBuffer();

	long cnt_word = 0;
	long cnt_file = 0;
	for(String p:postingList)
	{
	    out.append(p);
	    out.append(" \t");
	    long num =  Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
	    if(num >0)
		cnt_file++;
	    cnt_word += num;
	}

	if(cnt_file > 0)
	    out.insert(0, (double)(cnt_word)/(double)cnt_file + ", ");
	if(cnt_word > 0)
	    context.write(CurrentItem, new Text(out.toString()));
    }*/
}
