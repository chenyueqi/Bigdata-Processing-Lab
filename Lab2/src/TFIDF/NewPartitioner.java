/*
 * partitioner class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/28 19:00
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
//	String term = new String();
	/*partitioner according to <word#author> instead of <word#author#filename>*/
	String term[] = key.toString().split("#");
	return super.getPartition(new Text(term[0] + "#" + term[1]), value, numReduceTasks);
    }
}
