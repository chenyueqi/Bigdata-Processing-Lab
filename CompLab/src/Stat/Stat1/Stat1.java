/*
 * main class for Stat1
 * Project Name: 
 * Group Name: what the f**k
 * Created: Wei Liu (lw_nju@outlook.com)
 * Time: 2016/7/5 15:43
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class Stat1
{
    public static void main(String[] args) throws Exception
    {
    	System.out.println("input = " + args[0] + ", output = " + args[1]);

    	Configuration conf = new Configuration();
    	Job job = new Job(conf , "Stat1");

    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	job.setJarByClass(Stat1.class);
    	job.setMapperClass(Stat1Mapper.class);
    	job.setReducerClass(Stat1Reducer.class);
    	job.setCombinerClass(SumCombiner1.class);
    	job.setPartitionerClass(NewPartitioner1.class);

    	job.setOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
    	job.setOutputValueClass(Text.class);

    	job.waitForCompletion(true);
    }
}
