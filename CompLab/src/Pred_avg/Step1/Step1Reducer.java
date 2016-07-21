/*
 * Reducer class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created by: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/22 21:59
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class Step1Reducer extends Reducer<Text, IntWritable, Text, Text>
{
    static Text CurrentItem = new Text("**");
    static List<String> pastList = new ArrayList<String>();
    static StringBuffer out = new StringBuffer();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
	    int sum = 0;

	    for(IntWritable val : values)
	        sum += val.get();

	    Text urlHour = new Text();

	    String[] temp = key.toString().split("#");


	    String url = temp[0];
	    String hour = temp[1];
	    String date = temp[2];

	    urlHour.set(url + "#" + hour);


	    if(!CurrentItem.equals(urlHour) && (!CurrentItem.equals("**")))
	    {
            int avg = 0;
	        for(String p : pastList)
	        {
		        avg += Integer.parseInt(p.split("#")[1]);
	        }
            avg = avg/14;

            out.append((int)sum + "");
	        context.write(CurrentItem, new Text(out.toString()));
	        pastList = new ArrayList<String>();
            out = new StringBuffer();
	    }

	    CurrentItem = new Text(urlHour);
        if(Integer.parseInt(date) == 22)
            out.append(sum+"#");
        else
	        pastList.add(date + "#" + sum);
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
        int sum = 0;
	
	    StringBuffer out = new StringBuffer();
	    for(String p : pastList)
	    {
		    sum += Integer.parseInt(p.split("#")[1]);
	    }
        sum = sum/14;

	    context.write(CurrentItem, new Text(sum+""));
    }
}
