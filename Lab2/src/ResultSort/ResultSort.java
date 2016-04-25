/*
 * main class to sort Inverted Index's results
 * Project Name: Inverted Index Table for Hadoop(Lab2)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/25 14:56
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class ResultSort
{
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();

	Job job = new Job(conf, "Result Sort");
	FileInputFormat.addInputPath(job , new Path(args[0]));
	FileOutputFormat.setOutputPath(job , new Path(args[1]));

	job.setJarByClass(ResultSort.class);
	job.setMapperClass(ResultSortMapper.class);
	//set number of reducer task as 1
	job.setNumReduceTasks(1);

	job.setOutputKeyClass(DoubleWritable.class);
	job.setOutputValueClass(Text.class);

	System.exit(job.waitForCompletion(true)?0:1);
    }

}
