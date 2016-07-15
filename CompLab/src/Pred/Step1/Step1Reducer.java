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
    static ArrayList<String> pastList = new ArrayList<String>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
	int sum = 0;

	for(IntWritable val : values)
	    sum += val.get();

	Text urlTime = new Text();
	Text valueDate = new Text();
	
	String url = key.toString().split("#")[0];
	String time = key.toString().split("#")[1];
	String date = key.toString().split("#")[2];

	urlTime.set(url + "#" + time);


	if(!CurrentItem.equals(urlTime) && (!CurrentItem.equals(" ")))
	{
	    int size = pastList.size();
	    double result = 0;

	    for(int i = 0 ; i < size ; i++)
	    {
		int dayi = Integer.parseInt(pastList.get(i).split("#")[1]);
		double value = (double)Integer.parseInt(pastList.get(i).split("#")[0]);

		double baseFun = 1;
		for(int j = 0 ; j < size ; j++)
		{
		    if(i != j)
		    {
			int day = Integer.parseInt(pastList.get(j).split("#")[1]);
			baseFun *= ((double)(22-day)/(double)(dayi - day));
		    }
		}
		result += baseFun * value;
	    }

	    context.write(new Text(CurrentItem.toString().split("#")[0]), new Text((int)(result) + ""));
	    pastList = new ArrayList<String>();
	}

	CurrentItem = new Text(urlTime);
	pastList.add(sum + "#" + date);
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	    int size = pastList.size();
	    double result = 0;

	    for(int i = 0 ; i < size ; i++)
	    {
		int dayi = Integer.parseInt(pastList.get(i).split("#")[1]);
		double value = (double)Integer.parseInt(pastList.get(i).split("#")[0]);

		double baseFun = 1;
		for(int j = 0 ; j < size ; j++)
		{
		    if(i != j)
		    {
			int day = Integer.parseInt(pastList.get(j).split("#")[1]);

			baseFun *= ((double)(22-day)/(double)(dayi - day));
		    }

		}

		result += baseFun * value;
	    }

	    context.write(new Text(CurrentItem.toString().split("#")[0]), new Text((int)(result) + ""));
    }
}
