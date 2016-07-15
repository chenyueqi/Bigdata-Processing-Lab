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

public class Stat2Reducer extends Reducer<Text, IntWritable, Text, Text>
{
    private MultipleOutputs out;
    
    protected void setup(Context context) throws IOException,InterruptedException
    {
        out = new MultipleOutputs<Text,Text>(context);
    }

    private String get_time(Text text)
    {
        if(text.toString().contains("@"))
        {
            int time1 = Integer.parseInt(text.toString().split("#")[1].split("@")[0]);
            int time2 = time1 + 1;
            String IP = text.toString().split("#")[1].split("@")[1];

            return new String(time1+":00-"+time2+":00\t"+IP);
        }
        else
        {
            String IP = text.toString().split("#")[1];
            return new String(IP);
        }
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        String IP = new String();
        if(key.toString().contains("@"))
            IP = key.toString().split("#")[1].split("@")[1];
        else
            IP = key.toString().split("#")[1];

		int sum =0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		Text result = new Text();
		result.set(get_time(key)+":"+sum);
		out.write("IPResult",result, new Text(""),IP+".txt");
    }
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        out.close();
    }
}
