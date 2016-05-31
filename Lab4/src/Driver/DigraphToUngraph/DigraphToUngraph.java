/*
 * main class for translation directed graph to undirected graph
 * Project Name: Lab4
 * Group Name: what the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/5/20 11:59
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class DigraphToUngraph
{
    public static void main(String[] args) throws Exception
    {
	System.out.println("input = " + args[0] + ", output = " + args[1]);

	Configuration conf = new Configuration();
	Job job = new Job(conf , "Translation Di->Un");

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

			
	job.setJarByClass(DigraphToUngraph.class);
	job.setNumReduceTasks(16);
	job.setMapperClass(DigraphToUngraphMapper.class);
	job.setReducerClass(DigraphToUngraphReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.waitForCompletion(true);
    }
}
