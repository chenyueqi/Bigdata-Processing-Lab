/*
 * Reducer class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created by: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/22 21:59
*/

import java.io.IOException;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class Step2Reducer extends Reducer<Text, LongWritable, Text, Text>
{
    static int M = 0;
    static double sum = 0;
    static Text CurrentItem = new Text(" ");

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
	int N = 0;
	double localsum = 0;
	for(LongWritable val: values)
	{
	    localsum += val.get();
	    N = N + 1;
	}

	sum += Math.sqrt(localsum/N);

    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	context.write(new Text("RMSE"), new Text(sum/24 + ""));
    }
}
