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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class Step1
{
    public static void main(String[] args) throws Exception
    {
    	System.out.println("input = " + args[0] + ", output = " + args[1]);

    	Configuration conf = new Configuration();
    	Job job = new Job(conf , "Prediction Step1");

    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	job.setJarByClass(Step1.class);
	job.setNumReduceTasks(8);
    	job.setMapperClass(Step1Mapper.class);
    	job.setReducerClass(Step1Reducer.class);
    	job.setCombinerClass(SumCombiner.class);
    	job.setPartitionerClass(NewPartitioner.class);

        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

	System.exit(job.waitForCompletion(true)?0:1);
    }
}
