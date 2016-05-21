/*
 * main class for counting the number triangle of a undirected graph
 * Project Name: Lab4
 * Group Name: what the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/5/20 13:30
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

public class InNeed
{
    public static void main(String[] args) throws Exception
    {
	System.out.println("input = " + args[0] + ", output = " + args[1]);

	Configuration conf = new Configuration();
	Job job = new Job(conf , "needed edge");

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

			
	job.setJarByClass(InNeed.class);
	job.setMapperClass(InNeedMapper.class);
	job.setReducerClass(InNeedReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	System.exit(job.waitForCompletion(true)?0:1);
    }
}
