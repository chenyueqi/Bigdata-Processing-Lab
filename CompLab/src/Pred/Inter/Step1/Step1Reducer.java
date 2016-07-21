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
    static Text CurrentItem = new Text(" ");
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


	if(!CurrentItem.equals(urlHour) && (!CurrentItem.equals(" ")))
	{
	    double result = 0;

	    for(String p1: pastList)
	    {
		int day1 = Integer.parseInt(p1.split("#")[0]);
		long value = Long.parseLong(p1.split("#")[1]);

		double baseFunUp = 1;
		double baseFunDown = 1;

		for(String p2: pastList)
		{
		    int day2 = Integer.parseInt(p2.split("#")[0]);

		    if(day1 == day2)
			continue;

		    baseFunUp *= (double)(22 - day2);
		    baseFunDown *= (double)(day1 - day2);
		}
		
		result += (baseFunUp/baseFunDown * (double)value);
	    }

	    if(result > 0)
	    {
		out.append((int)result + "");
		context.write(CurrentItem, new Text(out.toString()));
	    }
	    pastList = new ArrayList<String>();
	    out = new StringBuffer();
	}

	CurrentItem.set(urlHour);
	if(Integer.parseInt(date) > 20)
	{
	    if(Integer.parseInt(date) == 22)
		out.append(sum+"#");
	    else
	    	pastList.add(date + "#" + sum);
	}
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	    double result = 0;

	    for(String p1: pastList)
	    {
		int day1 = Integer.parseInt(p1.split("#")[0]);
		long value = Long.parseLong(p1.split("#")[1]);

		double baseFunUp = 1;
		double baseFunDown = 1;

		for(String p2: pastList)
		{
		    int day2 = Integer.parseInt(p2.split("#")[0]);

		    if(day1 == day2)
			continue;

		    baseFunUp *= (double)(22 - day2);
		    baseFunDown *= (double)(day1 - day2);
		}
		
		result += (baseFunUp/baseFunDown * (double)value);
	    }

	    if(result > 0)
	    {
		out.append((int)result + "");
		context.write(CurrentItem, new Text(out.toString()));
	    }
    }
}
