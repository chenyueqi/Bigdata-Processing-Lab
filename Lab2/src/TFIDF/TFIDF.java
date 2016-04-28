/*
 * main class for TFIDF
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/28 18:57
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class TFIDF
{
    public static void main(String[] args) throws Exception
    {
	System.out.println("input = " + args[0] + ", output = " + args[1]);

	/*environment configuration*/
	Configuration conf = new Configuration();

	Job job = new Job(conf, "TFIDF");

	/*HDFS API*/
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	
	job.setJarByClass(TFIDF.class);
	job.setMapperClass(TFIDFMapper.class);
	job.setCombinerClass(SumCombiner.class);
	job.setPartitionerClass(NewPartitioner.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(TFIDFReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);


	System.exit(job.waitForCompletion(true)?0:1);
    }
}

