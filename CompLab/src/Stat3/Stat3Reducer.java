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

public class Stat3Reducer extends Reducer<Text, IntWritable, Text, Text>
{
    private MultipleOutputs out;
    
    protected void setup(Context context) throws IOException,InterruptedException
    {
        out = new MultipleOutputs<Text,Text>(context);
    }

    private String getfilename(String str)
    {
        String[] names = str.split("/");
        String ret = new String("");
        for(int i=0;i<names.length;i++)
        {
            if(i < 2)
                ret = ret + names[i];
            else
                ret = ret + "-" + names[i];
        }
        return ret;
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        String interfaces = new String();
        if(key.toString().contains("@"))
            interfaces = key.toString().split("@")[1];
        else
            interfaces = key.toString().split("#")[1];

		int sum =0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		Text result = new Text();
		result.set(key.toString()+":"+sum);
		out.write("Interface",result, new Text(""),getfilename(interfaces)+".txt");
    }
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        out.close();
    }
}
