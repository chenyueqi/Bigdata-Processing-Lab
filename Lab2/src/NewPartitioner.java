/*
 * partitioner class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/22 21:48
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NewPartitioner extends HashPartitioner<Text, IntWritable>
{
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks)
    {
	String term = new String();
	term = key.toString().split(",")[0];
	return super.getPartition(new Text(term), value, numReduceTasks);
    }
}
